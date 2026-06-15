using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using Usf.Core.Messaging;
using Usf.Transport.RabbitMq.Tests.TestSupport;
using Xunit;

namespace Usf.Transport.RabbitMq.Tests.Unit;

public sealed class RabbitMqTopologyRuntimeTests
{
    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    public async Task StartAsync_CreatesOneConsumerPerQueueOnEachConfiguredChannel(int channelCount)
    {
        var channels = Enumerable.Range(0, channelCount)
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
        using var serviceProvider = new ServiceCollection().BuildServiceProvider();
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
