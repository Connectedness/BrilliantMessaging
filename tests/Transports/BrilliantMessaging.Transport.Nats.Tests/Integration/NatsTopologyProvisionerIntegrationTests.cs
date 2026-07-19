using System;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Transport.Nats.Tests.TestSupport;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Xunit;

namespace BrilliantMessaging.Transport.Nats.Tests.Integration;

[Collection<NatsCollection>]
public sealed class NatsTopologyProvisionerIntegrationTests : IAsyncLifetime
{
    private readonly NatsFixture _fixture;

    public NatsTopologyProvisionerIntegrationTests(NatsFixture fixture)
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
    public async Task AssertOnlyProvisioningSucceedsWhenJetStreamResourcesAlreadyExist()
    {
        await using NatsConnection connection = new (new NatsOpts { Url = _fixture.ConnectionString });
        NatsJSContext jetStream = new (connection);
        await jetStream.CreateOrUpdateStreamAsync(
            new StreamConfig("ORDERS", ["orders.*"]),
            TestContext.Current.CancellationToken
        );
        await jetStream.CreateOrUpdateConsumerAsync(
            "ORDERS",
            new ConsumerConfig("orders-worker")
            {
                DurableName = "orders-worker",
                FilterSubject = "orders.placed",
                AckPolicy = ConsumerConfigAckPolicy.Explicit,
                AckWait = NatsTopologyBuilderDefaults.DefaultAckWait,
                // 2 x the default MaxDeliver of 5: the server carries shutdown-interruption headroom.
                MaxDeliver = 10,
                MaxAckPending = 1024
            },
            TestContext.Current.CancellationToken
        );

        ServiceCollection services = new ();
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology => topology
                   .UseServer(_fixture.ConnectionString)
                   .Provisioning(NatsTopologyProvisioningMode.AssertOnly)
                   .Stream("ORDERS", stream => stream.Subject("orders.*"))
                   .Consume(
                        "ORDERS",
                        "orders-worker",
                        consumer => consumer
                           .FilterSubject("orders.placed")
                           .Handle<OrderPlaced, OrderPlacedHandler>()
                    )
            );
        await using var provider = services.BuildServiceProvider();

        var provisioner = provider.GetRequiredService<ITopologyProvisioner>();
        var act = () => provisioner.ProvisionAsync(TestContext.Current.CancellationToken);

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task CreateOrUpdateProvisioningPreservesWildcardConsumerFilter()
    {
        ServiceCollection services = new ();
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology => topology
                   .UseServer(_fixture.ConnectionString)
                   .Stream("ORDERS", stream => stream.Subject("orders.>"))
                   .Consume(
                        "ORDERS",
                        "orders-worker",
                        consumer => consumer
                           .FilterSubject("orders.*")
                           .Handle<OrderPlaced, OrderPlacedHandler>()
                    )
            );
        await using var provider = services.BuildServiceProvider();

        var provisioner = provider.GetRequiredService<ITopologyProvisioner>();
        await provisioner.ProvisionAsync(TestContext.Current.CancellationToken);

        await using NatsConnection connection = new (new NatsOpts { Url = _fixture.ConnectionString });
        NatsJSContext jetStream = new (connection);
        var consumer = await jetStream.GetConsumerAsync(
            "ORDERS",
            "orders-worker",
            TestContext.Current.CancellationToken
        );

        consumer.Info.Config.FilterSubject.Should().Be("orders.*");
    }

    [Fact]
    public async Task CreateOrUpdateProvisioningAllowsDisjointWorkQueueConsumers()
    {
        ServiceCollection services = new ();
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology => topology
                   .UseServer(_fixture.ConnectionString)
                   .Stream(
                        "ORDERS",
                        stream => stream
                           .Subject("orders.>")
                           .Retention(NatsStreamRetention.WorkQueue)
                    )
                   .Consume(
                        "ORDERS",
                        "orders-us",
                        consumer => consumer
                           .FilterSubject("orders.us.*")
                           .Handle<OrderPlaced, OrderPlacedHandler>()
                    )
                   .Consume(
                        "ORDERS",
                        "orders-eu",
                        consumer => consumer
                           .FilterSubject("orders.eu.*")
                           .Handle<OrderPlaced, OrderPlacedHandler>()
                    )
            );
        await using var provider = services.BuildServiceProvider();

        var provisioner = provider.GetRequiredService<ITopologyProvisioner>();
        await provisioner.ProvisionAsync(TestContext.Current.CancellationToken);

        await using NatsConnection connection = new (new NatsOpts { Url = _fixture.ConnectionString });
        NatsJSContext jetStream = new (connection);
        var usConsumer = await jetStream.GetConsumerAsync(
            "ORDERS",
            "orders-us",
            TestContext.Current.CancellationToken
        );
        var euConsumer = await jetStream.GetConsumerAsync(
            "ORDERS",
            "orders-eu",
            TestContext.Current.CancellationToken
        );

        usConsumer.Info.Config.FilterSubject.Should().Be("orders.us.*");
        euConsumer.Info.Config.FilterSubject.Should().Be("orders.eu.*");
    }

    [Fact]
    public async Task AssertOnlyProvisioningFailsWhenJetStreamResourcesAreMissing()
    {
        ServiceCollection services = new ();
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology => topology
                   .UseServer(_fixture.ConnectionString)
                   .Provisioning(NatsTopologyProvisioningMode.AssertOnly)
                   .Stream("ORDERS", stream => stream.Subject("orders.*"))
                   .Consume(
                        "ORDERS",
                        "orders-worker",
                        consumer => consumer.Handle<OrderPlaced, OrderPlacedHandler>()
                    )
            );
        await using var provider = services.BuildServiceProvider();

        var provisioner = provider.GetRequiredService<ITopologyProvisioner>();
        var act = () => provisioner.ProvisionAsync(TestContext.Current.CancellationToken);

        var exception = await act.Should().ThrowAsync<TopologyValidationException>();
        exception.Which.ValidationErrors.Should()
           .Contain(error => error.Contains("stream 'ORDERS' was not found", StringComparison.Ordinal))
           .And.Contain(
                error => error.Contains("consumer 'orders-worker' was not found", StringComparison.Ordinal)
            );
    }

    [Fact]
    public async Task AssertOnlyProvisioningReportsMissingConsumerOnExistingStream()
    {
        await using NatsConnection connection = new (new NatsOpts { Url = _fixture.ConnectionString });
        NatsJSContext jetStream = new (connection);
        await jetStream.CreateOrUpdateStreamAsync(
            new StreamConfig("ORDERS", ["orders.*"]),
            TestContext.Current.CancellationToken
        );

        ServiceCollection services = new ();
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology => topology
                   .UseServer(_fixture.ConnectionString)
                   .Provisioning(NatsTopologyProvisioningMode.AssertOnly)
                   .Stream("ORDERS", stream => stream.Subject("orders.*"))
                   .Consume(
                        "ORDERS",
                        "orders-worker",
                        consumer => consumer.Handle<OrderPlaced, OrderPlacedHandler>()
                    )
            );
        await using var provider = services.BuildServiceProvider();

        var provisioner = provider.GetRequiredService<ITopologyProvisioner>();
        var act = () => provisioner.ProvisionAsync(TestContext.Current.CancellationToken);

        var exception = await act.Should().ThrowAsync<TopologyValidationException>();
        var validationError = exception.Which.ValidationErrors.Should().ContainSingle().Which;
        validationError.Should()
           .Be("NATS consumer 'orders-worker' was not found on stream 'ORDERS', but the topology declares it.");
    }

    [Fact]
    public async Task AssertOnlyProvisioningReportsJetStreamResourceMismatches()
    {
        await using NatsConnection connection = new (new NatsOpts { Url = _fixture.ConnectionString });
        NatsJSContext jetStream = new (connection);
        await jetStream.CreateOrUpdateStreamAsync(
            new StreamConfig("ORDERS", ["orders.actual"])
            {
                Storage = StreamConfigStorage.Memory,
                Retention = StreamConfigRetention.Interest,
                DuplicateWindow = TimeSpan.FromMinutes(1),
                MaxAge = TimeSpan.FromMinutes(1),
                MaxMsgSize = 128,
                NumReplicas = 1
            },
            TestContext.Current.CancellationToken
        );
        await jetStream.CreateOrUpdateConsumerAsync(
            "ORDERS",
            new ConsumerConfig("orders-worker")
            {
                DurableName = "orders-worker",
                FilterSubject = "orders.actual",
                AckPolicy = ConsumerConfigAckPolicy.All,
                DeliverPolicy = ConsumerConfigDeliverPolicy.New,
                AckWait = TimeSpan.FromSeconds(5),
                MaxDeliver = 2,
                MaxAckPending = 64
            },
            TestContext.Current.CancellationToken
        );

        ServiceCollection services = new ();
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology => topology
                   .UseServer(_fixture.ConnectionString)
                   .Provisioning(NatsTopologyProvisioningMode.AssertOnly)
                   .Stream(
                        "ORDERS",
                        stream => stream
                           .Subject("orders.expected")
                           .Storage(NatsStreamStorage.File)
                           .Retention(NatsStreamRetention.Limits)
                           .DuplicateWindow(TimeSpan.FromMinutes(2))
                           .MaxAge(TimeSpan.FromMinutes(2))
                           .MaxMessageSize(256)
                           .Replicas(2)
                    )
                   .Consume(
                        "ORDERS",
                        "orders-worker",
                        consumer => consumer
                           .AckWait(TimeSpan.FromSeconds(10))
                           .MaxDeliver(5)
                           .MaxAckPending(128)
                           .Handle<OrderPlaced, OrderPlacedHandler>()
                    )
            );
        await using var provider = services.BuildServiceProvider();

        var provisioner = provider.GetRequiredService<ITopologyProvisioner>();
        var act = () => provisioner.ProvisionAsync(TestContext.Current.CancellationToken);

        var exception = await act.Should().ThrowAsync<TopologyValidationException>();
        exception.Which.ValidationErrors.Should()
           .Contain(error => error.Contains("subjects", StringComparison.Ordinal))
           .And.Contain(error => error.Contains("storage", StringComparison.Ordinal))
           .And.Contain(error => error.Contains("retention", StringComparison.Ordinal))
           .And.Contain(error => error.Contains("replicas", StringComparison.Ordinal))
           .And.Contain(error => error.Contains("duplicate window", StringComparison.Ordinal))
           .And.Contain(error => error.Contains("max age", StringComparison.Ordinal))
           .And.Contain(error => error.Contains("max message size", StringComparison.Ordinal))
           .And.Contain(error => error.Contains("AckWait", StringComparison.Ordinal))
           .And.Contain(error => error.Contains("MaxDeliver", StringComparison.Ordinal))
           .And.Contain(error => error.Contains("MaxAckPending", StringComparison.Ordinal))
           .And.Contain(error => error.Contains("filter subject", StringComparison.Ordinal))
           .And.Contain(error => error.Contains("AckPolicy", StringComparison.Ordinal))
           .And.Contain(error => error.Contains("DeliverPolicy", StringComparison.Ordinal));
    }
}
