using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;
using BrilliantMessaging.Transport.RabbitMq.Inbound;
using BrilliantMessaging.Transport.RabbitMq.Tests.TestSupport;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using Xunit;

namespace BrilliantMessaging.Transport.RabbitMq.Tests.Unit;

public sealed class RabbitMqTopologyRuntimeTests
{
    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    public async Task StartAsync_CreatesOneConsumerPerQueueOnEachConfiguredChannel(int channelCount)
    {
        var channels = Enumerable
           .Range(0, channelCount)
           .Select(static _ => new TestRabbitMqChannel())
           .ToArray();
        var connection = new TestRabbitMqConnection();

        foreach (var channel in channels)
        {
            connection.EnqueueChannel(channel.Object);
        }

        RabbitMqConnectionProvider connectionProvider = new (_ => Task.FromResult(connection.Object));
        var topologyBuilder = new RabbitMqTopologyBuilder();
        topologyBuilder.UseConnectionFactory(static _ => new ConnectionFactory());
        topologyBuilder.Queue("inbound");
        topologyBuilder.Consume(
            "inbound",
            consumer => consumer
               .ChannelCount(channelCount)
               .PrefetchCount(7)
               .Handle<ValidationMessageA, ValidationMessageAHandler>()
               .Handle<ValidationMessageB, ValidationMessageBHandler>()
        );
        RabbitMqTopologyCompiler compiler = new (
            RabbitMqCloudEventsTestFactory.CreateRegistry(),
            NullLoggerFactory.Instance,
            static _ => null,
            static _ => true
        );
        await using var topology = compiler.Compile(
            Topology.DefaultName,
            topologyBuilder.Build(),
            connectionProvider
        );
        await using var serviceProvider = new ServiceCollection().BuildServiceProvider();
        var runtime = new RabbitMqTopologyRuntime(topology, serviceProvider.GetRequiredService<IServiceScopeFactory>());

        await runtime.StartAsync(TestContext.Current.CancellationToken);

        channels.Should().OnlyContain(
            channel => channel.BasicConsumeCallCount == 1 &&
                       channel.ConsumedQueues.Count == 1 &&
                       channel.ConsumedQueues[0] == "inbound" &&
                       channel.LastPrefetchCount == 7
        );
        connection.CreateChannelOptions.Should().HaveCount(channelCount);

        await runtime.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task DeliveryFailureBeforePipeline_RecordsConsumedMessageWithErrorType()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        using var recorder = new InboundDiagnosticsRecorder();
        var channel = new TestRabbitMqChannel();
        var connection = new TestRabbitMqConnection();
        connection.EnqueueChannel(channel.Object);
        RabbitMqConnectionProvider connectionProvider = new (_ => Task.FromResult(connection.Object));
        var topologyBuilder = new RabbitMqTopologyBuilder();
        topologyBuilder.UseConnectionFactory(static _ => new ConnectionFactory());
        topologyBuilder.Queue("inbound");
        topologyBuilder.Consume(
            "inbound",
            consumer => consumer.Handle<ValidationMessageA, ValidationMessageAHandler>()
        );
        RabbitMqTopologyCompiler compiler = new (
            RabbitMqCloudEventsTestFactory.CreateRegistry(),
            NullLoggerFactory.Instance,
            static _ => null,
            static _ => true
        );
        await using var topology = compiler.Compile(
            Topology.DefaultName,
            topologyBuilder.Build(),
            connectionProvider
        );
        var services = new ServiceCollection();
        services.AddSingleton(RabbitMqCloudEventsTestFactory.CreateRegistry());
        services.AddSingleton<CloudEventsInboundMessageInspector>();
        await using var serviceProvider = services.BuildServiceProvider();
        var runtime = new RabbitMqTopologyRuntime(topology, serviceProvider.GetRequiredService<IServiceScopeFactory>());

        await runtime.StartAsync(cancellationToken);
        await channel.DeliverAsync(
            "consumer-1",
            42,
            redelivered: false,
            "events",
            "unknown",
            new BasicProperties
            {
                ContentType = "application/json",
                Headers = new Dictionary<string, object?>
                {
                    ["cloudEvents:id"] = "message-42",
                    ["cloudEvents:specversion"] = "1.0",
                    ["cloudEvents:source"] = "/tests/rabbitmq",
                    ["cloudEvents:type"] = "tests.rabbitmq.unknown",
                    ["cloudEvents:time"] = DateTimeOffset.UtcNow.ToString("O")
                }
            },
            "{}"u8.ToArray(),
            cancellationToken
        );

        recorder.ConsumedMessages.Should().ContainSingle().Which.Should().Contain(
            new KeyValuePair<string, object?>(MessagingSemanticConventions.MessagingSystem, "rabbitmq"),
            new KeyValuePair<string, object?>(MessagingSemanticConventions.MessagingDestinationName, "inbound"),
            new KeyValuePair<string, object?>(
                MessagingSemanticConventions.MessagingOperationName,
                MessagingSemanticConventions.ProcessOperation
            ),
            new KeyValuePair<string, object?>(
                MessagingSemanticConventions.ErrorType,
                MessagingSemanticConventions.ErrorTypeOther
            )
        );
        recorder.Durations.Should().BeEmpty();
        channel.BasicNackCallCount.Should().Be(1);
        channel.LastNackRequeue.Should().BeFalse();

        await runtime.StopAsync(cancellationToken);
    }

    [Fact]
    public async Task DeliveryMatchedByRecognizer_DispatchesToEndpoint()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var channel = new TestRabbitMqChannel();
        var connection = new TestRabbitMqConnection();
        connection.EnqueueChannel(channel.Object);
        RabbitMqConnectionProvider connectionProvider = new (_ => Task.FromResult(connection.Object));
        var topologyBuilder = new RabbitMqTopologyBuilder();
        topologyBuilder.UseConnectionFactory(static _ => new ConnectionFactory());
        topologyBuilder.Queue("inbound");
        topologyBuilder.Consume(
            "inbound",
            consumer => consumer
               .UseInspectors(chain => chain.WhenHeader("x-kind", "validation-a").As<ValidationMessageA>())
               .Handle<ValidationMessageA, RecordingValidationMessageAHandler>(
                    handler => handler.WithDeserializer<RawDeserializer>()
                )
        );
        RabbitMqTopologyCompiler compiler = new (
            RabbitMqCloudEventsTestFactory.CreateRegistry(),
            NullLoggerFactory.Instance,
            static _ => null,
            static _ => true
        );
        await using var topology = compiler.Compile(
            Topology.DefaultName,
            topologyBuilder.Build(),
            connectionProvider
        );
        var services = new ServiceCollection();
        HandlerInvocationSink sink = new ();
        services.AddSingleton(sink);
        services.AddSingleton<InboundDiagnosticsMiddleware>();
        services.AddSingleton<FrameworkMessageAcknowledgementMiddleware>();
        services.AddSingleton<MessageDeserializationMiddleware>();
        services.AddSingleton<RawDeserializer>();
        services.AddSingleton<RecordingValidationMessageAHandler>();
        await using var serviceProvider = services.BuildServiceProvider();
        var runtime = new RabbitMqTopologyRuntime(topology, serviceProvider.GetRequiredService<IServiceScopeFactory>());

        await runtime.StartAsync(cancellationToken);
        await channel.DeliverAsync(
            "consumer-1",
            42,
            redelivered: false,
            "events",
            "legacy",
            new BasicProperties
            {
                Headers = new Dictionary<string, object?>
                {
                    ["x-kind"] = "validation-a"
                }
            },
            "{}"u8.ToArray(),
            cancellationToken
        );

        sink.Invocations.Should().ContainSingle().Which.Should().Be("raw");
        channel.BasicAckCallCount.Should().Be(1);
        channel.BasicNackCallCount.Should().Be(0);

        await runtime.StopAsync(cancellationToken);
    }

    [Fact]
    public async Task MixedFormatQueue_RoutesCloudEventsAndRecognizedDeliveriesToOwnEndpoints()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var channel = new TestRabbitMqChannel();
        var connection = new TestRabbitMqConnection();
        connection.EnqueueChannel(channel.Object);
        RabbitMqConnectionProvider connectionProvider = new (_ => Task.FromResult(connection.Object));
        var topologyBuilder = new RabbitMqTopologyBuilder();
        topologyBuilder.UseConnectionFactory(static _ => new ConnectionFactory());
        topologyBuilder.Queue("inbound");
        topologyBuilder.Consume(
            "inbound",
            consumer => consumer
               .UseInspectors(
                    chain => chain
                       .CloudEvents()
                       .WhenHeader("x-kind", "legacy").As<ValidationMessageB>()
                )
               .Handle<ValidationMessageA, RecordingValidationMessageAHandler>(
                    handler => handler.WithDeserializer<RawValidationMessageADeserializer>()
                )
               .Handle<ValidationMessageB, RecordingValidationMessageBHandler>(
                    handler => handler.WithDeserializer<RawValidationMessageBDeserializer>()
                )
        );
        RabbitMqTopologyCompiler compiler = new (
            RabbitMqCloudEventsTestFactory.CreateRegistry(),
            NullLoggerFactory.Instance,
            static _ => null,
            static _ => true
        );
        await using var topology = compiler.Compile(
            Topology.DefaultName,
            topologyBuilder.Build(),
            connectionProvider
        );
        var services = new ServiceCollection();
        HandlerInvocationSink sink = new ();
        services.AddSingleton(sink);
        services.AddSingleton(RabbitMqCloudEventsTestFactory.CreateRegistry());
        services.AddSingleton<CloudEventsInboundMessageInspector>();
        services.AddSingleton<InboundDiagnosticsMiddleware>();
        services.AddSingleton<FrameworkMessageAcknowledgementMiddleware>();
        services.AddSingleton<MessageDeserializationMiddleware>();
        services.AddSingleton<RawValidationMessageADeserializer>();
        services.AddSingleton<RawValidationMessageBDeserializer>();
        services.AddSingleton<RecordingValidationMessageAHandler>();
        services.AddSingleton<RecordingValidationMessageBHandler>();
        await using var serviceProvider = services.BuildServiceProvider();
        var runtime = new RabbitMqTopologyRuntime(topology, serviceProvider.GetRequiredService<IServiceScopeFactory>());

        await runtime.StartAsync(cancellationToken);
        await channel.DeliverAsync(
            "consumer-1",
            42,
            redelivered: false,
            "events",
            "validation-a",
            new BasicProperties
            {
                ContentType = "application/json",
                Headers = new Dictionary<string, object?>
                {
                    ["cloudEvents:id"] = "message-42",
                    ["cloudEvents:specversion"] = "1.0",
                    ["cloudEvents:source"] = "/tests/rabbitmq",
                    ["cloudEvents:type"] = RabbitMqCloudEventsTestFactory.ValidationMessageADiscriminator,
                    ["cloudEvents:time"] = DateTimeOffset.UtcNow.ToString("O")
                }
            },
            "{}"u8.ToArray(),
            cancellationToken
        );
        await channel.DeliverAsync(
            "consumer-1",
            43,
            redelivered: false,
            "events",
            "legacy",
            new BasicProperties
            {
                Headers = new Dictionary<string, object?>
                {
                    ["x-kind"] = "legacy"
                }
            },
            "{}"u8.ToArray(),
            cancellationToken
        );

        sink.Invocations.Should().BeEquivalentTo("cloud-event-a", "recognized-b");
        channel.BasicAckCallCount.Should().Be(2);
        channel.BasicNackCallCount.Should().Be(0);

        await runtime.StopAsync(cancellationToken);
    }

    [Fact]
    public async Task DeliveryNotRecognizedByInspectorChain_NacksAsUnknownInboundMessage()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var channel = new TestRabbitMqChannel();
        var connection = new TestRabbitMqConnection();
        connection.EnqueueChannel(channel.Object);
        RabbitMqConnectionProvider connectionProvider = new (_ => Task.FromResult(connection.Object));
        var topologyBuilder = new RabbitMqTopologyBuilder();
        topologyBuilder.UseConnectionFactory(static _ => new ConnectionFactory());
        topologyBuilder.Queue("inbound");
        topologyBuilder.Consume(
            "inbound",
            consumer => consumer.Handle<ValidationMessageA, ValidationMessageAHandler>()
        );
        RabbitMqTopologyCompiler compiler = new (
            RabbitMqCloudEventsTestFactory.CreateRegistry(),
            NullLoggerFactory.Instance,
            static _ => null,
            static _ => true
        );
        await using var topology = compiler.Compile(
            Topology.DefaultName,
            topologyBuilder.Build(),
            connectionProvider
        );
        var services = new ServiceCollection();
        services.AddSingleton(RabbitMqCloudEventsTestFactory.CreateRegistry());
        services.AddSingleton<CloudEventsInboundMessageInspector>();
        await using var serviceProvider = services.BuildServiceProvider();
        var runtime = new RabbitMqTopologyRuntime(topology, serviceProvider.GetRequiredService<IServiceScopeFactory>());

        await runtime.StartAsync(cancellationToken);
        await channel.DeliverAsync(
            "consumer-1",
            42,
            redelivered: false,
            "events",
            "unrecognized",
            new BasicProperties
            {
                ContentType = "application/json",
                Headers = new Dictionary<string, object?>
                {
                    ["cloudEvents:specversion"] = "1.0"
                }
            },
            "{}"u8.ToArray(),
            cancellationToken
        );

        channel.BasicNackCallCount.Should().Be(1);
        channel.LastNackRequeue.Should().BeFalse();

        await runtime.StopAsync(cancellationToken);
    }

    [Fact]
    public async Task DeliveryWhoseInspectorResolvesUnknownDiscriminator_NacksAsUnknownInboundMessage()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var channel = new TestRabbitMqChannel();
        var connection = new TestRabbitMqConnection();
        connection.EnqueueChannel(channel.Object);
        RabbitMqConnectionProvider connectionProvider = new (_ => Task.FromResult(connection.Object));
        var topologyBuilder = new RabbitMqTopologyBuilder();
        topologyBuilder.UseConnectionFactory(static _ => new ConnectionFactory());
        topologyBuilder.Queue("inbound");
        topologyBuilder.Consume(
            "inbound",
            consumer => consumer
               .UseInspector<UnknownDiscriminatorInspector>()
               .Handle<ValidationMessageA, ValidationMessageAHandler>()
        );
        RabbitMqTopologyCompiler compiler = new (
            RabbitMqCloudEventsTestFactory.CreateRegistry(),
            NullLoggerFactory.Instance,
            static _ => null,
            static _ => true
        );
        await using var topology = compiler.Compile(
            Topology.DefaultName,
            topologyBuilder.Build(),
            connectionProvider
        );
        var services = new ServiceCollection();
        services.AddSingleton<UnknownDiscriminatorInspector>();
        await using var serviceProvider = services.BuildServiceProvider();
        var runtime = new RabbitMqTopologyRuntime(topology, serviceProvider.GetRequiredService<IServiceScopeFactory>());

        await runtime.StartAsync(cancellationToken);
        await channel.DeliverAsync(
            "consumer-1",
            42,
            redelivered: false,
            "events",
            "validation-a",
            new BasicProperties(),
            "{}"u8.ToArray(),
            cancellationToken
        );

        channel.BasicNackCallCount.Should().Be(1);
        channel.LastNackRequeue.Should().BeFalse();

        await runtime.StopAsync(cancellationToken);
    }

    [Fact]
    public async Task DeliveryWhoseInspectorResolvesMismatchedMessageType_NacksAsUnknownInboundMessage()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var channel = new TestRabbitMqChannel();
        var connection = new TestRabbitMqConnection();
        connection.EnqueueChannel(channel.Object);
        RabbitMqConnectionProvider connectionProvider = new (_ => Task.FromResult(connection.Object));
        var topologyBuilder = new RabbitMqTopologyBuilder();
        topologyBuilder.UseConnectionFactory(static _ => new ConnectionFactory());
        topologyBuilder.Queue("inbound");
        topologyBuilder.Consume(
            "inbound",
            consumer => consumer
               .UseInspector<MismatchedTypeInspector>()
               .Handle<ValidationMessageA, ValidationMessageAHandler>()
        );
        RabbitMqTopologyCompiler compiler = new (
            RabbitMqCloudEventsTestFactory.CreateRegistry(),
            NullLoggerFactory.Instance,
            static _ => null,
            static _ => true
        );
        await using var topology = compiler.Compile(
            Topology.DefaultName,
            topologyBuilder.Build(),
            connectionProvider
        );
        var services = new ServiceCollection();
        services.AddSingleton<MismatchedTypeInspector>();
        await using var serviceProvider = services.BuildServiceProvider();
        var runtime = new RabbitMqTopologyRuntime(topology, serviceProvider.GetRequiredService<IServiceScopeFactory>());

        await runtime.StartAsync(cancellationToken);
        await channel.DeliverAsync(
            "consumer-1",
            42,
            redelivered: false,
            "events",
            "validation-a",
            new BasicProperties(),
            "{}"u8.ToArray(),
            cancellationToken
        );

        channel.BasicNackCallCount.Should().Be(1);
        channel.LastNackRequeue.Should().BeFalse();

        await runtime.StopAsync(cancellationToken);
    }

    [Fact]
    public async Task DeliveryCancellationFromEventArgsToken_RequeuesWithoutFailureMetrics()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        using var recorder = new InboundDiagnosticsRecorder();
        var channel = new TestRabbitMqChannel();
        var connection = new TestRabbitMqConnection();
        connection.EnqueueChannel(channel.Object);
        RabbitMqConnectionProvider connectionProvider = new (_ => Task.FromResult(connection.Object));
        var topologyBuilder = new RabbitMqTopologyBuilder();
        topologyBuilder.UseConnectionFactory(static _ => new ConnectionFactory());
        topologyBuilder.Queue("inbound");
        topologyBuilder.Consume(
            "inbound",
            consumer => consumer
               .UseInspector<RawInspector>()
               .Handle<ValidationMessageA, CancellingValidationMessageAHandler>(
                    handler => handler
                       .WithDeserializer<RawDeserializer>()
                       .ManualAck()
                )
        );
        RabbitMqTopologyCompiler compiler = new (
            RabbitMqCloudEventsTestFactory.CreateRegistry(),
            NullLoggerFactory.Instance,
            static _ => null,
            static _ => true
        );
        await using var topology = compiler.Compile(
            Topology.DefaultName,
            topologyBuilder.Build(),
            connectionProvider
        );
        var services = new ServiceCollection();
        services.AddSingleton<InboundDiagnosticsMiddleware>();
        services.AddSingleton<FrameworkMessageAcknowledgementMiddleware>();
        services.AddSingleton<MessageDeserializationMiddleware>();
        services.AddSingleton<RawInspector>();
        services.AddSingleton<RawDeserializer>();
        services.AddSingleton<CancellingValidationMessageAHandler>();
        await using var serviceProvider = services.BuildServiceProvider();
        var runtime = new RabbitMqTopologyRuntime(topology, serviceProvider.GetRequiredService<IServiceScopeFactory>());

        await runtime.StartAsync(cancellationToken);
        using CancellationTokenSource deliveryCancellationTokenSource = new ();
        await deliveryCancellationTokenSource.CancelAsync();
        await channel.DeliverAsync(
            "consumer-1",
            42,
            redelivered: false,
            "events",
            "validation-a",
            new BasicProperties(),
            "{}"u8.ToArray(),
            deliveryCancellationTokenSource.Token
        );

        channel.BasicNackCallCount.Should().Be(1);
        channel.LastNackRequeue.Should().BeTrue();
        // A graceful-shutdown cancellation is an ordinary consumed-message increment with error.type absent.
        recorder.ConsumedMessages.Should().ContainSingle().Which.Should()
           .NotContain(tag => tag.Key == MessagingSemanticConventions.ErrorType);
        recorder.Durations.Should().ContainSingle().Which.Should()
           .NotContain(tag => tag.Key == MessagingSemanticConventions.ErrorType);

        await runtime.StopAsync(cancellationToken);
    }

    private sealed class ValidationMessageAHandler : IMessageHandler<ValidationMessageA>
    {
        public Task HandleAsync(
            ValidationMessageA message,
            IncomingMessageContext context,
            CancellationToken cancellationToken
        )
        {
            return Task.CompletedTask;
        }
    }

    private sealed class ValidationMessageBHandler : IMessageHandler<ValidationMessageB>
    {
        public Task HandleAsync(
            ValidationMessageB message,
            IncomingMessageContext context,
            CancellationToken cancellationToken
        )
        {
            return Task.CompletedTask;
        }
    }

    private sealed class RecordingValidationMessageAHandler : IMessageHandler<ValidationMessageA>
    {
        private readonly HandlerInvocationSink _sink;

        public RecordingValidationMessageAHandler(HandlerInvocationSink sink)
        {
            _sink = sink;
        }

        public Task HandleAsync(
            ValidationMessageA message,
            IncomingMessageContext context,
            CancellationToken cancellationToken
        )
        {
            _sink.Invocations.Add(message.Value);
            return Task.CompletedTask;
        }
    }

    private sealed class CancellingValidationMessageAHandler : IMessageHandler<ValidationMessageA>
    {
        public Task HandleAsync(
            ValidationMessageA message,
            IncomingMessageContext context,
            CancellationToken cancellationToken
        )
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }
    }

    private sealed class RawInspector : IInboundMessageInspector
    {
        public ValueTask<InboundMessageInspectionResult?> InspectAsync(
            TransportMessage transportMessage,
            CancellationToken cancellationToken = default
        )
        {
            return new ValueTask<InboundMessageInspectionResult?>(
                new InboundMessageInspectionResult(
                    RabbitMqCloudEventsTestFactory.ValidationMessageADiscriminator,
                    typeof(ValidationMessageA)
                )
            );
        }
    }

    private sealed class RecordingValidationMessageBHandler : IMessageHandler<ValidationMessageB>
    {
        private readonly HandlerInvocationSink _sink;

        public RecordingValidationMessageBHandler(HandlerInvocationSink sink)
        {
            _sink = sink;
        }

        public Task HandleAsync(
            ValidationMessageB message,
            IncomingMessageContext context,
            CancellationToken cancellationToken
        )
        {
            _sink.Invocations.Add(message.Value);
            return Task.CompletedTask;
        }
    }

    private sealed class RawValidationMessageADeserializer : IMessageDeserializer
    {
        public ValueTask<object?> DeserializeAsync(
            IncomingMessageContext context,
            CancellationToken cancellationToken = default
        )
        {
            return new ValueTask<object?>(new ValidationMessageA("cloud-event-a"));
        }
    }

    private sealed class RawValidationMessageBDeserializer : IMessageDeserializer
    {
        public ValueTask<object?> DeserializeAsync(
            IncomingMessageContext context,
            CancellationToken cancellationToken = default
        )
        {
            return new ValueTask<object?>(new ValidationMessageB("recognized-b"));
        }
    }

    private sealed class MismatchedTypeInspector : IInboundMessageInspector
    {
        public ValueTask<InboundMessageInspectionResult?> InspectAsync(
            TransportMessage transportMessage,
            CancellationToken cancellationToken = default
        )
        {
            // Resolves to the ValidationMessageA endpoint's discriminator, but reports an unrelated message type so
            // the runtime's endpoint/message-type assignability guard rejects the delivery.
            return new ValueTask<InboundMessageInspectionResult?>(
                new InboundMessageInspectionResult(
                    RabbitMqCloudEventsTestFactory.ValidationMessageADiscriminator,
                    typeof(ValidationMessageB)
                )
            );
        }
    }

    private sealed class UnknownDiscriminatorInspector : IInboundMessageInspector
    {
        public ValueTask<InboundMessageInspectionResult?> InspectAsync(
            TransportMessage transportMessage,
            CancellationToken cancellationToken = default
        )
        {
            // Resolves a discriminator that maps to no endpoint on the queue, so the runtime rejects the delivery.
            return new ValueTask<InboundMessageInspectionResult?>(
                new InboundMessageInspectionResult("tests.rabbitmq.does-not-exist", typeof(ValidationMessageA))
            );
        }
    }

    private sealed class RawDeserializer : IMessageDeserializer
    {
        public ValueTask<object?> DeserializeAsync(
            IncomingMessageContext context,
            CancellationToken cancellationToken = default
        )
        {
            return new ValueTask<object?>(new ValidationMessageA("raw"));
        }
    }

    private sealed class HandlerInvocationSink
    {
        public List<string> Invocations { get; } = [];
    }
}
