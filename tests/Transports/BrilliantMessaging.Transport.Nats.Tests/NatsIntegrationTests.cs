using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;
using BrilliantMessaging.Core.Messaging.Outbound;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Testcontainers.Nats;
using Xunit;

namespace BrilliantMessaging.Transport.Nats.Tests;

public sealed class NatsIntegrationTests
{
    [Fact]
    public async Task PublishConsumeAndProvisionDurableConsumer()
    {
        await using var container = new NatsBuilder("nats:2.11-alpine")
           .WithCommand("-js")
           .Build();
        await container.StartAsync(TestContext.Current.CancellationToken);

        RecordingProbe probe = new ();
        ServiceCollection services = new ();
        services.AddSingleton(probe);
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology => topology
                   .UseServer(container.GetConnectionString())
                   .Stream("ORDERS", stream => stream.Subject("orders.*"))
                   .Publish<OrderPlaced>(target => target.ToSubject("orders.placed"))
                   .Consume(
                        "ORDERS",
                        "orders-worker",
                        consumer => consumer
                           .FilterSubject("orders.placed")
                           .DeadLetterSubject("orders.dead")
                           .Handle<OrderPlaced, RecordingOrderPlacedHandler>()
                    )
            );
        await using var provider = services.BuildServiceProvider();

        foreach (var provisioner in provider.GetServices<ITopologyProvisioner>())
        {
            await provisioner.ProvisionAsync(TestContext.Current.CancellationToken);
        }

        var runtimes = provider.GetServices<ITopologyRuntime>().ToArray();
        foreach (var runtime in runtimes)
        {
            await runtime.StartAsync(TestContext.Current.CancellationToken);
        }

        try
        {
            var publisher = provider.GetRequiredService<IMessagePublisher>();
            await publisher.PublishMessageAsync(
                new OrderPlaced { OrderId = "order-1" },
                cancellationToken: TestContext.Current.CancellationToken
            );

            var received = await probe.WaitAsync(TimeSpan.FromSeconds(15), TestContext.Current.CancellationToken);
            received.OrderId.Should().Be("order-1");
        }
        finally
        {
            foreach (var runtime in runtimes)
            {
                await runtime.StopAsync(CancellationToken.None);
            }
        }
    }

    [Fact]
    public async Task HandlerFailureDeadLettersAndSettlesOriginal()
    {
        await using var container = new NatsBuilder("nats:2.11-alpine")
           .WithCommand("-js")
           .Build();
        await container.StartAsync(TestContext.Current.CancellationToken);

        RecordingProbe probe = new ();
        ServiceCollection services = new ();
        services.AddSingleton(probe);
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology => topology
                   .UseServer(container.GetConnectionString())
                   .Stream("ORDERS", stream => stream.Subject("orders.*"))
                   .Publish<OrderPlaced>(target => target.ToSubject("orders.placed"))
                   .Consume(
                        "ORDERS",
                        "orders-worker",
                        consumer => consumer
                           .FilterSubject("orders.placed")
                           .MaxDeliver(1)
                           .DeadLetterSubject("orders.dead")
                           .Handle<OrderPlaced, FailingOrderPlacedHandler>()
                    )
                   .Consume(
                        "ORDERS",
                        "dead-worker",
                        consumer => consumer
                           .FilterSubject("orders.dead")
                           .Handle<OrderPlaced, RecordingOrderPlacedHandler>()
                    )
            );
        await using var provider = services.BuildServiceProvider();

        foreach (var provisioner in provider.GetServices<ITopologyProvisioner>())
        {
            await provisioner.ProvisionAsync(TestContext.Current.CancellationToken);
        }

        var runtimes = provider.GetServices<ITopologyRuntime>().ToArray();
        foreach (var runtime in runtimes)
        {
            await runtime.StartAsync(TestContext.Current.CancellationToken);
        }

        try
        {
            var publisher = provider.GetRequiredService<IMessagePublisher>();
            await publisher.PublishMessageAsync(
                new OrderPlaced { OrderId = "order-dead" },
                cancellationToken: TestContext.Current.CancellationToken
            );

            var received = await probe.WaitAsync(TimeSpan.FromSeconds(15), TestContext.Current.CancellationToken);
            received.OrderId.Should().Be("order-dead");
        }
        finally
        {
            foreach (var runtime in runtimes)
            {
                await runtime.StopAsync(CancellationToken.None);
            }
        }
    }

    [Fact]
    public async Task RetryableFailureIsRedelivered()
    {
        await using var container = new NatsBuilder("nats:2.11-alpine")
           .WithCommand("-js")
           .Build();
        await container.StartAsync(TestContext.Current.CancellationToken);

        RedeliveryProbe probe = new ();
        ServiceCollection services = new ();
        services.AddSingleton(probe);
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology => topology
                   .UseServer(container.GetConnectionString())
                   .Stream("ORDERS", stream => stream.Subject("orders.*"))
                   .Publish<OrderPlaced>(target => target.ToSubject("orders.placed"))
                   .Consume(
                        "ORDERS",
                        "orders-worker",
                        consumer => consumer
                           .FilterSubject("orders.placed")
                           .MaxDeliver(2)
                           .AckWait(TimeSpan.FromSeconds(2))
                           .Handle<OrderPlaced, RedeliveringOrderPlacedHandler>()
                    )
            );
        await using var provider = services.BuildServiceProvider();

        foreach (var provisioner in provider.GetServices<ITopologyProvisioner>())
        {
            await provisioner.ProvisionAsync(TestContext.Current.CancellationToken);
        }

        var runtimes = provider.GetServices<ITopologyRuntime>().ToArray();
        foreach (var runtime in runtimes)
        {
            await runtime.StartAsync(TestContext.Current.CancellationToken);
        }

        try
        {
            var publisher = provider.GetRequiredService<IMessagePublisher>();
            await publisher.PublishMessageAsync(
                new OrderPlaced { OrderId = "order-retry" },
                cancellationToken: TestContext.Current.CancellationToken
            );

            var attempt = await probe.WaitAsync(TimeSpan.FromSeconds(15), TestContext.Current.CancellationToken);
            attempt.Should().Be(2);
        }
        finally
        {
            foreach (var runtime in runtimes)
            {
                await runtime.StopAsync(CancellationToken.None);
            }
        }
    }

    [Fact]
    public async Task SerializedPublishMapsCloudEventHeadersForNatsWire()
    {
        await using var container = new NatsBuilder("nats:2.11-alpine")
           .WithCommand("-js")
           .Build();
        await container.StartAsync(TestContext.Current.CancellationToken);

        ServiceCollection services = new ();
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology => topology
                   .UseServer(container.GetConnectionString())
                   .Stream("ORDERS", stream => stream.Subject("orders.*"))
                   .Publish<OrderPlaced>(target => target.ToSubject("orders.raw").UseMessageIdDeduplication())
            );
        await using var provider = services.BuildServiceProvider();

        foreach (var provisioner in provider.GetServices<ITopologyProvisioner>())
        {
            await provisioner.ProvisionAsync(TestContext.Current.CancellationToken);
        }

        await using NatsConnection connection = new (new NatsOpts { Url = container.GetConnectionString() });
        NatsJSContext jetStream = new (connection);
        await jetStream.CreateOrUpdateConsumerAsync(
            "ORDERS",
            new ConsumerConfig("wire-inspector")
            {
                DurableName = "wire-inspector",
                FilterSubject = "orders.raw",
                AckPolicy = ConsumerConfigAckPolicy.Explicit,
                DeliverPolicy = ConsumerConfigDeliverPolicy.All
            },
            TestContext.Current.CancellationToken
        );

        var target = provider.GetRequiredService<NatsTopology>().GetRequiredTarget<OrderPlaced>();
        SerializedMessage message = new (
            "body"u8.ToArray(),
            "application/json",
            "gzip",
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                ["cloudEvents:type"] = "tests.order.placed",
                ["traceparent"] = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00"
            },
            "message-raw",
            null
        );

        await target.PublishSerializedAsync(message, TestContext.Current.CancellationToken);

        var received = await ReadSingleMessageAsync(
            jetStream,
            "ORDERS",
            "wire-inspector",
            TestContext.Current.CancellationToken
        );
        received.Subject.Should().Be("orders.raw");
        received.Data.Should().Equal("body"u8.ToArray());
        var headers = GetHeaders(received);
        HeaderValue(headers, "ce-type").Should().Be("tests.order.placed");
        headers.ContainsKey("cloudEvents:type").Should().BeFalse();
        HeaderValue(headers, "content-type").Should().Be("application/json");
        HeaderValue(headers, "content-encoding").Should().Be("gzip");
        HeaderValue(headers, "message-id").Should().Be("message-raw");
        HeaderValue(headers, "traceparent").Should().Be("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00");
    }

    [Fact]
    public async Task MessageIdDeduplicationStoresOnlyOneMessageForSameCloudEventId()
    {
        await using var container = new NatsBuilder("nats:2.11-alpine")
           .WithCommand("-js")
           .Build();
        await container.StartAsync(TestContext.Current.CancellationToken);

        ServiceCollection services = new ();
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology => topology
                   .UseServer(container.GetConnectionString())
                   .Stream(
                        "ORDERS",
                        stream => stream.Subject("orders.*").DuplicateWindow(TimeSpan.FromMinutes(1))
                    )
                   .Publish<OrderPlaced>(
                        target => target.ToSubject("orders.dedup").UseMessageIdDeduplication()
                    )
            );
        await using var provider = services.BuildServiceProvider();

        foreach (var provisioner in provider.GetServices<ITopologyProvisioner>())
        {
            await provisioner.ProvisionAsync(TestContext.Current.CancellationToken);
        }

        await using NatsConnection connection = new (new NatsOpts { Url = container.GetConnectionString() });
        NatsJSContext jetStream = new (connection);
        await jetStream.CreateOrUpdateConsumerAsync(
            "ORDERS",
            new ConsumerConfig("dedup-inspector")
            {
                DurableName = "dedup-inspector",
                FilterSubject = "orders.dedup",
                AckPolicy = ConsumerConfigAckPolicy.Explicit,
                DeliverPolicy = ConsumerConfigDeliverPolicy.All
            },
            TestContext.Current.CancellationToken
        );

        var publisher = provider.GetRequiredService<IMessagePublisher>();
        var eventId = Guid.Parse("89abfdcb-fb6f-4203-870f-9dd019fbc907");
        OrderPlaced message = new () { Id = eventId, OrderId = "order-dedup" };

        await publisher.PublishMessageAsync(message, cancellationToken: TestContext.Current.CancellationToken);
        await publisher.PublishMessageAsync(message, cancellationToken: TestContext.Current.CancellationToken);

        var received = await ReadMessagesAsync(
            jetStream,
            "ORDERS",
            "dedup-inspector",
            2,
            TimeSpan.FromSeconds(2),
            TestContext.Current.CancellationToken
        );

        received.Should().ContainSingle();
        HeaderValue(GetHeaders(received[0]), "message-id").Should().Be(eventId.ToString());
    }

    [Fact]
    public async Task UnknownDiscriminatorIsDeadLetteredAndOriginalDeliveryIsTerminated()
    {
        await using var container = new NatsBuilder("nats:2.11-alpine")
           .WithCommand("-js")
           .Build();
        await container.StartAsync(TestContext.Current.CancellationToken);

        ServiceCollection services = new ();
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology => topology
                   .UseServer(container.GetConnectionString())
                   .Stream("ORDERS", stream => stream.Subject("orders.*"))
                   .Consume(
                        "ORDERS",
                        "orders-worker",
                        consumer => consumer
                           .FilterSubject("orders.unknown")
                           .DeadLetterSubject("orders.dead")
                           .Handle<OrderPlaced, OrderPlacedHandler>()
                    )
            );
        await using var provider = services.BuildServiceProvider();

        foreach (var provisioner in provider.GetServices<ITopologyProvisioner>())
        {
            await provisioner.ProvisionAsync(TestContext.Current.CancellationToken);
        }

        await using NatsConnection connection = new (new NatsOpts { Url = container.GetConnectionString() });
        NatsJSContext jetStream = new (connection);
        await jetStream.CreateOrUpdateConsumerAsync(
            "ORDERS",
            new ConsumerConfig("dead-inspector")
            {
                DurableName = "dead-inspector",
                FilterSubject = "orders.dead",
                AckPolicy = ConsumerConfigAckPolicy.Explicit,
                DeliverPolicy = ConsumerConfigDeliverPolicy.All
            },
            TestContext.Current.CancellationToken
        );

        var runtimes = provider.GetServices<ITopologyRuntime>().ToArray();
        foreach (var runtime in runtimes)
        {
            await runtime.StartAsync(TestContext.Current.CancellationToken);
        }

        try
        {
            NatsHeaders headers = new ()
            {
                ["ce-type"] = "tests.unknown",
                ["message-id"] = "message-unknown"
            };
            await jetStream.PublishAsync(
                "orders.unknown",
                "unknown"u8.ToArray(),
                serializer: null,
                opts: null,
                headers,
                TestContext.Current.CancellationToken
            );

            var deadLetter = await ReadSingleMessageAsync(
                jetStream,
                "ORDERS",
                "dead-inspector",
                TestContext.Current.CancellationToken
            );

            deadLetter.Subject.Should().Be("orders.dead");
            deadLetter.Data.Should().Equal("unknown"u8.ToArray());
            HeaderValue(GetHeaders(deadLetter), "ce-type").Should().Be("tests.unknown");
        }
        finally
        {
            foreach (var runtime in runtimes)
            {
                await runtime.StopAsync(CancellationToken.None);
            }
        }
    }

    private static async Task<INatsJSMsg<byte[]>> ReadSingleMessageAsync(
        NatsJSContext jetStream,
        string streamName,
        string durableName,
        CancellationToken cancellationToken
    )
    {
        var messages = await ReadMessagesAsync(
            jetStream,
            streamName,
            durableName,
            1,
            TimeSpan.FromSeconds(10),
            cancellationToken
        );

        return messages.Should().ContainSingle().Which;
    }

    private static async Task<List<INatsJSMsg<byte[]>>> ReadMessagesAsync(
        NatsJSContext jetStream,
        string streamName,
        string durableName,
        int maxMessages,
        TimeSpan timeout,
        CancellationToken cancellationToken
    )
    {
        using var timeoutSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutSource.CancelAfter(timeout);
        var consumer = await jetStream
           .GetConsumerAsync(streamName, durableName, timeoutSource.Token)
           .ConfigureAwait(false);
        NatsJSConsumeOpts options = new ()
        {
            MaxMsgs = maxMessages,
            Expires = timeout
        };
        List<INatsJSMsg<byte[]>> messages = [];
        try
        {
            await foreach (var message in consumer
                              .ConsumeAsync<byte[]>(serializer: null, options, timeoutSource.Token)
                              .ConfigureAwait(false))
            {
                messages.Add(message);
                await message.AckAsync(cancellationToken: timeoutSource.Token).ConfigureAwait(false);
                if (messages.Count == maxMessages)
                {
                    break;
                }
            }
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return messages;
        }

        return messages;
    }

    private static NatsHeaders GetHeaders(INatsJSMsg<byte[]> message)
    {
        message.Should().NotBeNull();
        var natsMessage = (INatsMsg) message;
        natsMessage.Headers.Should().NotBeNull();
        return natsMessage.Headers!;
    }

    private static string? HeaderValue(NatsHeaders headers, string name)
    {
        return headers.TryGetValue(name, out var value) ? value.ToString() : null;
    }

    private sealed class RecordingProbe
    {
        private readonly TaskCompletionSource<OrderPlaced> _received =
            new (TaskCreationOptions.RunContinuationsAsynchronously);

        public void Record(OrderPlaced message)
        {
            _received.TrySetResult(message);
        }

        public async Task<OrderPlaced> WaitAsync(TimeSpan timeout, CancellationToken cancellationToken)
        {
            var delay = Task.Delay(timeout, cancellationToken);
            var completed = await Task.WhenAny(_received.Task, delay).ConfigureAwait(false);
            if (completed == delay)
            {
                throw new TimeoutException("Timed out waiting for a NATS message.");
            }

            return await _received.Task.ConfigureAwait(false);
        }
    }

    private sealed class RedeliveryProbe
    {
        private readonly TaskCompletionSource<uint> _received =
            new (TaskCreationOptions.RunContinuationsAsynchronously);

        private int _seen;

        public void Observe(IncomingMessageContext context)
        {
            if (Interlocked.Increment(ref _seen) == 1)
            {
                throw new RetryMessageException();
            }

            _received.TrySetResult(context.Transport.DeliveryAttempt);
        }

        public async Task<uint> WaitAsync(TimeSpan timeout, CancellationToken cancellationToken)
        {
            var delay = Task.Delay(timeout, cancellationToken);
            var completed = await Task.WhenAny(_received.Task, delay).ConfigureAwait(false);
            if (completed == delay)
            {
                throw new TimeoutException("Timed out waiting for a NATS redelivery.");
            }

            return await _received.Task.ConfigureAwait(false);
        }
    }

    private sealed class RecordingOrderPlacedHandler : IMessageHandler<OrderPlaced>
    {
        private readonly RecordingProbe _probe;

        public RecordingOrderPlacedHandler(RecordingProbe probe)
        {
            _probe = probe;
        }

        public Task HandleAsync(
            OrderPlaced message,
            IncomingMessageContext context,
            CancellationToken cancellationToken = default
        )
        {
            _probe.Record(message);
            return Task.CompletedTask;
        }
    }

    private sealed class FailingOrderPlacedHandler : IMessageHandler<OrderPlaced>
    {
        public Task HandleAsync(
            OrderPlaced message,
            IncomingMessageContext context,
            CancellationToken cancellationToken = default
        )
        {
            throw new InvalidOperationException("Simulated handler failure.");
        }
    }

    private sealed class RedeliveringOrderPlacedHandler : IMessageHandler<OrderPlaced>
    {
        private readonly RedeliveryProbe _probe;

        public RedeliveringOrderPlacedHandler(RedeliveryProbe probe)
        {
            _probe = probe;
        }

        public Task HandleAsync(
            OrderPlaced message,
            IncomingMessageContext context,
            CancellationToken cancellationToken = default
        )
        {
            _probe.Observe(context);
            return Task.CompletedTask;
        }
    }
}
