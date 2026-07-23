using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;
using BrilliantMessaging.Core.Messaging.Outbound;
using BrilliantMessaging.Transport.InMemory;
using BrilliantMessaging.Transport.Nats;
using BrilliantMessaging.Transport.RabbitMq;
using BrilliantMessaging.Transport.RabbitMq.Inbound;
using BrilliantMessaging.Transports.Integration.Tests.TestSupport;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using Xunit;

namespace BrilliantMessaging.Transports.Integration.Tests;

[Collection<MultiTransportCollection>]
public sealed class MultiTransportIntegrationTests
{
    private static readonly TimeSpan FlowTimeout = TimeSpan.FromSeconds(30);
    private readonly MultiTransportFixture _fixture;

    public MultiTransportIntegrationTests(MultiTransportFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task HostedServices_DriveMessageFlowAcrossAllRegisteredTransports()
    {
        var suffix = Guid.NewGuid().ToString("N");
        var inMemoryTopology = $"memory-{suffix}";
        var rabbitMqTopology = $"rabbit-{suffix}";
        var natsTopology = $"nats-{suffix}";
        var memoryTopic = $"orders.{suffix}";
        var rabbitExchange = $"orders-{suffix}";
        var rabbitDeadLetterExchange = $"orders-dead-{suffix}";
        var rabbitQueue = $"orders-work-{suffix}";
        var rabbitDeadLetterQueue = $"orders-rejected-{suffix}";
        var rabbitRoutingKey = $"orders.{suffix}";
        var rabbitDeadLetterRoutingKey = $"orders.rejected.{suffix}";
        var natsStream = $"REPORTS_{suffix.ToUpperInvariant()}";
        var natsSubject = $"reports.{suffix}";
        var natsDurable = $"reports_worker_{suffix}";
        MessageFlowProbe probe = new ();
        MessageFlowTopologies messageFlowTopologies = new (rabbitMqTopology, natsTopology);
        ServiceCollection services = new ();
        services.AddSingleton(probe);
        services.AddSingleton(messageFlowTopologies);
        services
           .AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests/multi-transport")
           .MapMessageContracts(
                contracts =>
                {
                    contracts.Map<IncomingOrder>("tests.multi.incoming-order");
                    contracts.Map<TransformedOrder>("tests.multi.transformed-order");
                    contracts.Map<RejectedOrderReport>("tests.multi.rejected-order-report");
                }
            )
           .AddInMemoryTopology(
                inMemoryTopology,
                topology => topology
                   .Topic(memoryTopic)
                   .Publish<IncomingOrder>(target => target.ToTopic(memoryTopic))
                   .Consume(
                        memoryTopic,
                        consumer => consumer.Handle<IncomingOrder, TransformingOrderHandler>()
                    )
            )
           .AddRabbitMqTopology(
                rabbitMqTopology,
                topology => topology
                   .UseConnectionFactory(
                        _ => new ConnectionFactory
                        {
                            Uri = new Uri(_fixture.RabbitMqConnectionString)
                        }
                    )
                   .Exchange(rabbitExchange, ExchangeType.Direct)
                   .Exchange(rabbitDeadLetterExchange, ExchangeType.Direct)
                   .Queue(
                        rabbitQueue,
                        queue => queue
                           .WithDeadLetterExchange(rabbitDeadLetterExchange)
                           .WithDeadLetterRoutingKey(rabbitDeadLetterRoutingKey)
                    )
                   .Queue(rabbitDeadLetterQueue)
                   .QueueBinding(rabbitExchange, rabbitQueue, rabbitRoutingKey)
                   .QueueBinding(
                        rabbitDeadLetterExchange,
                        rabbitDeadLetterQueue,
                        rabbitDeadLetterRoutingKey
                    )
                   .Publish<TransformedOrder>(
                        target => target
                           .ToDirectExchange(rabbitExchange, rabbitRoutingKey)
                           .WithSerializer<CloudEventMessageSerializer>()
                    )
                   .Consume(
                        rabbitQueue,
                        consumer => consumer.Handle<TransformedOrder, RejectingOrderHandler>()
                    )
                   .Consume(
                        rabbitDeadLetterQueue,
                        consumer => consumer.Handle<TransformedOrder, DeadLetterReportingHandler>()
                    )
            )
           .AddNatsTopology(
                natsTopology,
                topology => topology
                   .UseServer(_fixture.NatsConnectionString)
                   .Stream(natsStream, stream => stream.Subject(natsSubject))
                   .Publish<RejectedOrderReport>(target => target.ToSubject(natsSubject))
                   .Consume(
                        natsStream,
                        natsDurable,
                        consumer => consumer
                           .FilterSubject(natsSubject)
                           .Handle<RejectedOrderReport, ReportRecordingHandler>()
                    )
            );

        await using var serviceProvider = services.BuildServiceProvider();
        var topologyNames = serviceProvider.GetRequiredService<ITopologyRegistry>().Names;
        topologyNames.Should().BeEquivalentTo(inMemoryTopology, rabbitMqTopology, natsTopology);

        var provisioners = serviceProvider.GetServices<ITopologyProvisioner>().ToArray();
        provisioners.Should().HaveCount(2);
        provisioners.Should().ContainSingle(provisioner => provisioner is RabbitMqTopologyProvisioner);
        provisioners.Should().ContainSingle(provisioner => provisioner is NatsTopologyProvisioner);

        var runtimes = serviceProvider.GetServices<ITopologyRuntime>().ToArray();
        runtimes.Should().HaveCount(3);
        runtimes.Should().ContainSingle(runtime => runtime is InMemoryTopologyRuntime);
        runtimes.Should().ContainSingle(runtime => runtime is RabbitMqTopologyRuntime);
        runtimes.Should().ContainSingle(runtime => runtime is NatsTopologyRuntime);

        var hostedServices = serviceProvider.GetServices<IHostedService>().ToArray();
        hostedServices.Should().HaveCount(2);
        hostedServices.Should().ContainSingle(service => service is TopologyProvisioningHostedService);
        hostedServices.Should().ContainSingle(service => service is TopologyRuntimeHostedService);

        using var timeoutSource =
            CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        timeoutSource.CancelAfter(FlowTimeout);
        var cancellationToken = timeoutSource.Token;

        try
        {
            foreach (var hostedService in hostedServices)
            {
                await hostedService.StartAsync(cancellationToken);
            }

            var publisher = serviceProvider.GetRequiredService<IMessagePublisher>();
            await publisher
               .ForTopology(inMemoryTopology)
               .PublishMessageAsync(
                    new IncomingOrder { OrderId = "order-83" },
                    cancellationToken: cancellationToken
                );

            var transformed = await probe.WaitForTransformedAsync(cancellationToken);
            var report = await probe.WaitForReportAsync(cancellationToken);

            transformed.OrderId.Should().Be("order-83");
            transformed.Description.Should().Be("transformed:order-83");
            report.OrderId.Should().Be(transformed.OrderId);
            report.RejectedDescription.Should().Be(transformed.Description);
            report.Outcome.Should().Be("dead-lettered");
        }
        finally
        {
            foreach (var hostedService in hostedServices.Reverse())
            {
                await hostedService.StopAsync(CancellationToken.None);
            }
        }
    }
}
