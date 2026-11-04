using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;
using BrilliantMessaging.Core.Messaging.Outbound;
using BrilliantMessaging.Transport.Nats.Inbound;
using BrilliantMessaging.Transport.Nats.Tests.TestSupport;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Xunit;

namespace BrilliantMessaging.Transport.Nats.Tests.Integration;

[Collection<NatsCollection>]
public sealed class NatsInspectorChainIntegrationTests : IAsyncLifetime
{
    private const string S3EventJson =
        """
        {
          "Records": [
            {
              "eventVersion": "2.6",
              "eventSource": "aws:s3",
              "eventName": "ObjectCreated:Put",
              "s3": {
                "s3SchemaVersion": "1.0",
                "bucket": { "name": "uploads" },
                "object": { "key": "red+flower.jpg", "size": 1024 }
              }
            }
          ]
        }
        """;

    private readonly NatsFixture _fixture;

    public NatsInspectorChainIntegrationTests(NatsFixture fixture)
    {
        _fixture = fixture;
    }

    public async ValueTask InitializeAsync()
    {
        await _fixture.ResetAsync(TestContext.Current.CancellationToken);
    }

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }

    [Fact]
    public async Task RawS3EventIsRecognizedBySubjectAndHandledThroughCustomDeserializer()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        Probe<S3Notification> probe = new ();
        await using var host = await StartAsync(
            services =>
            {
                services.AddSingleton(probe);
                services.AddSingleton<S3NotificationDeserializer>();
            },
            topology => topology.Consume(
                "BUCKETS",
                "bucket-worker",
                consumer => consumer
                   .FilterSubject("bucket.events")
                   .UseInspectors(
                        chain => chain
                           .CloudEvents()
                           .When(static message => message.Source == "bucket.events").As<S3Notification>("s3.event")
                    )
                   .Handle<S3Notification, ProbeHandler<S3Notification>>(
                        handler => handler.WithDeserializer<S3NotificationDeserializer>()
                    )
            )
        );

        await host.JetStream.PublishAsync(
            "bucket.events",
            Encoding.UTF8.GetBytes(S3EventJson),
            serializer: null,
            opts: null,
            headers: null,
            cancellationToken
        );

        var notification = await probe.WaitAsync(cancellationToken);

        notification.EventName.Should().Be("ObjectCreated:Put");
        notification.Bucket.Should().Be("uploads");
        notification.Key.Should().Be("red+flower.jpg");
    }

    [Fact]
    public async Task HeaderRecognizerMatchesMappedHeaderNameInDeclarationOrder()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        Probe<RawEvent> probe = new ();
        await using var host = await StartAsync(
            services =>
            {
                services.AddSingleton(probe);
                services.AddSingleton<RawEventDeserializer>();
            },
            topology => topology.Consume(
                "BUCKETS",
                "bucket-worker",
                consumer => consumer
                   .UseInspectors(
                        chain => chain
                           .WhenHeader("ce-type").As<OrderPlaced>("tests.wire-name")
                           .WhenHeader("cloudEvents:type").As<RawEvent>("tests.raw")
                           .WhenHeader("x-kind", "raw").As<OrderPlaced>("tests.shadowed")
                    )
                   .Handle<OrderPlaced, NoOpHandler<OrderPlaced>>()
                   .Handle<RawEvent, ProbeHandler<RawEvent>>(
                        handler => handler.WithDeserializer<RawEventDeserializer>()
                    )
            )
        );

        await host.JetStream.PublishAsync(
            "bucket.events",
            "raw"u8.ToArray(),
            serializer: null,
            opts: null,
            new NatsHeaders
            {
                ["ce-type"] = "external.type",
                ["x-kind"] = "raw"
            },
            cancellationToken
        );

        var received = await probe.WaitAsync(cancellationToken);

        received.Should().Be(new RawEvent());
    }

    [Fact]
    public async Task CloudEventsEntryResolvesTopologyLocalContractAheadOfRecognizer()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        Probe<OrderPlaced> probe = new ();
        await using var host = await StartAsync(
            services => services.AddSingleton(probe),
            topology => topology
               .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("local.order.placed"))
               .Publish<OrderPlaced>(target => target.ToSubject("bucket.orders"))
               .Consume(
                    "BUCKETS",
                    "bucket-worker",
                    consumer => consumer
                       .UseInspectors(
                            chain => chain
                               .CloudEvents()
                               .When(static _ => true).As<RawEvent>("tests.raw")
                        )
                       .Handle<OrderPlaced, ProbeHandler<OrderPlaced>>()
                       .Handle<RawEvent, NoOpHandler<RawEvent>>()
                )
        );

        await host.Provider.GetRequiredService<IMessagePublisher>().PublishMessageAsync(
            new OrderPlaced { OrderId = "order-1" },
            cancellationToken: cancellationToken
        );

        var received = await probe.WaitAsync(cancellationToken);

        received.OrderId.Should().Be("order-1");
    }

    [Fact]
    public async Task ScopedInspectorIsResolvedFromPerDeliveryScope()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        Probe<RawEvent> probe = new ();
        ScopedInspectorTracker tracker = new ();
        await using var host = await StartAsync(
            services =>
            {
                services.AddSingleton(probe);
                services.AddSingleton(tracker);
                services.AddSingleton<RawEventDeserializer>();
            },
            topology => topology
               .MapMessageContracts(contracts => contracts.Map<RawEvent>("tests.raw"))
               .Consume(
                    "BUCKETS",
                    "bucket-worker",
                    consumer => consumer
                       .UseInspector<ScopedInspector>(ServiceLifetime.Scoped)
                       .Handle<RawEvent, ProbeHandler<RawEvent>>(
                            handler => handler.WithDeserializer<RawEventDeserializer>()
                        )
                )
        );

        await host.PublishRawAsync("bucket.a", cancellationToken);
        await probe.WaitAsync(cancellationToken);
        await host.PublishRawAsync("bucket.b", cancellationToken);
        await probe.WaitForCountAsync(2, cancellationToken);

        tracker.InstanceIds.Distinct().Should().HaveCount(2);
    }

    [Theory]
    [InlineData(typeof(ThrowingInspector), "bucket.throwing")]
    [InlineData(typeof(UnrecognizingInspector), "bucket.unrecognized")]
    [InlineData(typeof(UnknownDiscriminatorInspector), "bucket.unknown")]
    [InlineData(typeof(MismatchedTypeInspector), "bucket.mismatched")]
    public async Task UnroutableDeliveryIsDeadLetteredWithoutConsumingRetries(Type inspectorType, string subject)
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        await using var host = await StartAsync(
            services =>
            {
                services.AddSingleton<ThrowingInspector>();
                services.AddSingleton<UnrecognizingInspector>();
                services.AddSingleton<UnknownDiscriminatorInspector>();
                services.AddSingleton<MismatchedTypeInspector>();
            },
            topology => topology.Consume(
                "BUCKETS",
                "bucket-worker",
                consumer =>
                {
                    consumer
                       .FilterSubject(subject)
                       .DeadLetterSubject("bucket.dead")
                       .Handle<OrderPlaced, NoOpHandler<OrderPlaced>>();
                    UseInspector(consumer, inspectorType);
                }
            )
        );
        await host.JetStream.CreateOrUpdateConsumerAsync(
            "BUCKETS",
            new ConsumerConfig("dead-reader")
            {
                DurableName = "dead-reader",
                FilterSubject = "bucket.dead",
                AckPolicy = ConsumerConfigAckPolicy.Explicit,
                DeliverPolicy = ConsumerConfigDeliverPolicy.All
            },
            cancellationToken
        );

        await host.PublishRawAsync(subject, cancellationToken);

        var deadLetter = await host.ReadSingleAsync("dead-reader", cancellationToken);

        deadLetter.Subject.Should().Be("bucket.dead");
        deadLetter.Data.Should().Equal("raw"u8.ToArray());
        var worker = await host.WaitForNoAckPendingAsync("bucket-worker", cancellationToken);
        worker.Info.Delivered.ConsumerSeq.Should().Be(1);
    }

    private static void UseInspector(NatsInboundConsumerBuilder consumer, Type inspectorType)
    {
        if (inspectorType == typeof(ThrowingInspector))
        {
            consumer.UseInspector<ThrowingInspector>();
        }
        else if (inspectorType == typeof(UnrecognizingInspector))
        {
            consumer.UseInspector<UnrecognizingInspector>();
        }
        else if (inspectorType == typeof(UnknownDiscriminatorInspector))
        {
            consumer.UseInspector<UnknownDiscriminatorInspector>();
        }
        else if (inspectorType == typeof(MismatchedTypeInspector))
        {
            consumer.UseInspector<MismatchedTypeInspector>();
        }
        else
        {
            throw new ArgumentOutOfRangeException(nameof(inspectorType), inspectorType, "Unsupported inspector type.");
        }
    }

    private async Task<TestHost> StartAsync(
        Action<ServiceCollection> configureServices,
        Action<NatsTopologyBuilder> configureTopology
    )
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        ServiceCollection services = new ();
        configureServices(services);
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology =>
                {
                    topology
                       .UseServer(_fixture.ConnectionString)
                       .Stream("BUCKETS", stream => stream.Subject("bucket.*"));
                    configureTopology(topology);
                }
            );
        var provider = services.BuildServiceProvider();

        foreach (var provisioner in provider.GetServices<ITopologyProvisioner>())
        {
            await provisioner.ProvisionAsync(cancellationToken);
        }

        var runtimes = provider.GetServices<ITopologyRuntime>().ToArray();
        foreach (var runtime in runtimes)
        {
            await runtime.StartAsync(cancellationToken);
        }

        NatsConnection connection = new (new NatsOpts { Url = _fixture.ConnectionString });
        return new TestHost(provider, runtimes, connection);
    }

    private sealed class TestHost : IAsyncDisposable
    {
        private readonly NatsConnection _connection;
        private readonly IReadOnlyList<ITopologyRuntime> _runtimes;

        public TestHost(ServiceProvider provider, IReadOnlyList<ITopologyRuntime> runtimes, NatsConnection connection)
        {
            Provider = provider;
            _runtimes = runtimes;
            _connection = connection;
            JetStream = new NatsJSContext(connection);
        }

        public ServiceProvider Provider { get; }

        public NatsJSContext JetStream { get; }

        public async ValueTask DisposeAsync()
        {
            foreach (var runtime in _runtimes)
            {
                await runtime.StopAsync(CancellationToken.None);
            }

            await _connection.DisposeAsync();
            await Provider.DisposeAsync();
        }

        public async Task PublishRawAsync(string subject, CancellationToken cancellationToken)
        {
            await JetStream.PublishAsync(
                subject,
                "raw"u8.ToArray(),
                serializer: null,
                opts: null,
                headers: null,
                cancellationToken
            );
        }

        public async Task<INatsJSConsumer> WaitForNoAckPendingAsync(
            string durableName,
            CancellationToken cancellationToken
        )
        {
            var deadline = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(10);
            while (DateTimeOffset.UtcNow < deadline)
            {
                var consumer = await JetStream.GetConsumerAsync("BUCKETS", durableName, cancellationToken);
                if (consumer.Info.NumAckPending == 0)
                {
                    return consumer;
                }

                await Task.Delay(TimeSpan.FromMilliseconds(200), cancellationToken);
            }

            throw new TimeoutException($"Timed out waiting for consumer '{durableName}' to settle.");
        }

        public async Task<INatsJSMsg<byte[]>> ReadSingleAsync(string durableName, CancellationToken cancellationToken)
        {
            var consumer = await JetStream.GetConsumerAsync("BUCKETS", durableName, cancellationToken);
            var deadline = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(15);
            while (DateTimeOffset.UtcNow < deadline)
            {
                var message = await consumer.NextAsync<byte[]>(
                    opts: new NatsJSNextOpts { Expires = TimeSpan.FromSeconds(2) },
                    cancellationToken: cancellationToken
                );
                if (message is { } received)
                {
                    await received.AckAsync(cancellationToken: cancellationToken);
                    return received;
                }
            }

            throw new TimeoutException($"Timed out waiting for a message on consumer '{durableName}'.");
        }
    }

    private sealed class Probe<TMessage>
    {
        private readonly List<TMessage> _messages = [];
        private readonly SemaphoreSlim _received = new (0);

        public void Record(TMessage message)
        {
            lock (_messages)
            {
                _messages.Add(message);
            }

            _received.Release();
        }

        public async Task<TMessage> WaitAsync(CancellationToken cancellationToken)
        {
            await WaitForCountAsync(1, cancellationToken);
            lock (_messages)
            {
                return _messages[0];
            }
        }

        public async Task WaitForCountAsync(int count, CancellationToken cancellationToken)
        {
            var deadline = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(15);
            while (true)
            {
                lock (_messages)
                {
                    if (_messages.Count >= count)
                    {
                        return;
                    }
                }

                var remaining = deadline - DateTimeOffset.UtcNow;
                if (remaining <= TimeSpan.Zero || !await _received.WaitAsync(remaining, cancellationToken))
                {
                    throw new TimeoutException("Timed out waiting for a NATS message.");
                }
            }
        }
    }

    private sealed class ProbeHandler<TMessage> : IMessageHandler<TMessage>
    {
        private readonly Probe<TMessage> _probe;

        public ProbeHandler(Probe<TMessage> probe)
        {
            _probe = probe;
        }

        public Task HandleAsync(
            TMessage message,
            IncomingMessageContext context,
            CancellationToken cancellationToken = default
        )
        {
            _probe.Record(message);
            return Task.CompletedTask;
        }
    }

    private sealed class NoOpHandler<TMessage> : IMessageHandler<TMessage>
    {
        public Task HandleAsync(
            TMessage message,
            IncomingMessageContext context,
            CancellationToken cancellationToken = default
        )
        {
            return Task.CompletedTask;
        }
    }

    private sealed record S3Notification(string EventName, string Bucket, string Key);

    private sealed record RawEvent;

    private sealed class S3NotificationDeserializer : IMessageDeserializer
    {
        public ValueTask<object?> DeserializeAsync(
            IncomingMessageContext context,
            CancellationToken cancellationToken = default
        )
        {
            using var document = JsonDocument.Parse(context.Transport.Body);
            var record = document.RootElement.GetProperty("Records")[0];
            var s3 = record.GetProperty("s3");
            return new ValueTask<object?>(
                new S3Notification(
                    record.GetProperty("eventName").GetString()!,
                    s3.GetProperty("bucket").GetProperty("name").GetString()!,
                    s3.GetProperty("object").GetProperty("key").GetString()!
                )
            );
        }
    }

    private sealed class RawEventDeserializer : IMessageDeserializer
    {
        public ValueTask<object?> DeserializeAsync(
            IncomingMessageContext context,
            CancellationToken cancellationToken = default
        )
        {
            return new ValueTask<object?>(new RawEvent());
        }
    }

    private sealed class ScopedInspectorTracker
    {
        private readonly List<Guid> _instanceIds = [];

        public IReadOnlyList<Guid> InstanceIds
        {
            get
            {
                lock (_instanceIds)
                {
                    return _instanceIds.ToArray();
                }
            }
        }

        public void Record(Guid instanceId)
        {
            lock (_instanceIds)
            {
                _instanceIds.Add(instanceId);
            }
        }
    }

    private sealed class ScopedInspector : IInboundMessageInspector
    {
        private readonly Guid _instanceId = Guid.NewGuid();
        private readonly ScopedInspectorTracker _tracker;

        public ScopedInspector(ScopedInspectorTracker tracker)
        {
            _tracker = tracker;
        }

        public ValueTask<InboundMessageInspectionResult?> InspectAsync(
            TransportMessage transportMessage,
            CancellationToken cancellationToken = default
        )
        {
            _tracker.Record(_instanceId);
            return new ValueTask<InboundMessageInspectionResult?>(
                new InboundMessageInspectionResult("tests.raw", typeof(RawEvent))
            );
        }
    }

    private sealed class ThrowingInspector : IInboundMessageInspector
    {
        public ValueTask<InboundMessageInspectionResult?> InspectAsync(
            TransportMessage transportMessage,
            CancellationToken cancellationToken = default
        )
        {
            throw new InvalidOperationException("The inspector failed.");
        }
    }

    private sealed class UnrecognizingInspector : IInboundMessageInspector
    {
        public ValueTask<InboundMessageInspectionResult?> InspectAsync(
            TransportMessage transportMessage,
            CancellationToken cancellationToken = default
        )
        {
            return new ValueTask<InboundMessageInspectionResult?>((InboundMessageInspectionResult?) null);
        }
    }

    private sealed class UnknownDiscriminatorInspector : IInboundMessageInspector
    {
        public ValueTask<InboundMessageInspectionResult?> InspectAsync(
            TransportMessage transportMessage,
            CancellationToken cancellationToken = default
        )
        {
            return new ValueTask<InboundMessageInspectionResult?>(
                new InboundMessageInspectionResult("tests.unknown", typeof(OrderPlaced))
            );
        }
    }

    private sealed class MismatchedTypeInspector : IInboundMessageInspector
    {
        public ValueTask<InboundMessageInspectionResult?> InspectAsync(
            TransportMessage transportMessage,
            CancellationToken cancellationToken = default
        )
        {
            return new ValueTask<InboundMessageInspectionResult?>(
                new InboundMessageInspectionResult("tests.order.placed", typeof(RawEvent))
            );
        }
    }
}
