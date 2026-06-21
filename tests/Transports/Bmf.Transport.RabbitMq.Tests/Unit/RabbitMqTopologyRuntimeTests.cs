using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Bmf.Core.Messaging;
using Bmf.Core.Messaging.Inbound;
using Bmf.Transport.RabbitMq.Inbound;
using Bmf.Transport.RabbitMq.Tests.TestSupport;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using Xunit;

namespace Bmf.Transport.RabbitMq.Tests.Unit;

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
    public async Task DeliveryFailureBeforePipeline_RecordsAttemptAndFailureMetrics()
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

        recorder.Attempts.Should().ContainSingle().Which.Should().Contain(
            new KeyValuePair<string, object?>(InboundDiagnostics.SourceTagName, "inbound"),
            new KeyValuePair<string, object?>(InboundDiagnostics.TransportNameTagName, "rabbitmq"),
            new KeyValuePair<string, object?>(InboundDiagnostics.OutcomeTagName, "failure")
        );
        recorder.Failures.Should().ContainSingle().Which.Should().Contain(
            new KeyValuePair<string, object?>(InboundDiagnostics.SourceTagName, "inbound"),
            new KeyValuePair<string, object?>(InboundDiagnostics.TransportNameTagName, "rabbitmq"),
            new KeyValuePair<string, object?>(InboundDiagnostics.OutcomeTagName, "failure")
        );
        recorder.Durations.Should().BeEmpty();
        channel.BasicNackCallCount.Should().Be(1);
        channel.LastNackRequeue.Should().BeFalse();

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
}
