using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Transport.Nats.Inbound;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Testcontainers.Nats;
using Xunit;

namespace BrilliantMessaging.Transport.Nats.Tests;

public sealed class NatsTopologyProvisionerTests
{
    [Theory]
    [InlineData(NatsStreamStorage.File, StreamConfigStorage.File)]
    [InlineData(NatsStreamStorage.Memory, StreamConfigStorage.Memory)]
    public void ToStreamConfig_MapsStoragePolicy(NatsStreamStorage storage, StreamConfigStorage expected)
    {
        NatsStreamDefinition stream = new (
            "ORDERS",
            ["orders.*"],
            null,
            null,
            null,
            storage,
            NatsStreamRetention.Limits,
            1
        );

        var config = NatsTopologyProvisioner.ToStreamConfig(stream);

        config.Storage.Should().Be(expected);
    }

    [Theory]
    [InlineData(NatsStreamRetention.Limits, StreamConfigRetention.Limits)]
    [InlineData(NatsStreamRetention.Interest, StreamConfigRetention.Interest)]
    [InlineData(NatsStreamRetention.WorkQueue, StreamConfigRetention.Workqueue)]
    public void ToStreamConfig_MapsRetentionPolicy(NatsStreamRetention retention, StreamConfigRetention expected)
    {
        NatsStreamDefinition stream = new (
            "ORDERS",
            ["orders.*"],
            null,
            null,
            null,
            NatsStreamStorage.File,
            retention,
            1
        );

        var config = NatsTopologyProvisioner.ToStreamConfig(stream);

        config.Retention.Should().Be(expected);
    }

    [Fact]
    public void ToStreamConfig_MapsOptionalLimits()
    {
        NatsStreamDefinition stream = new (
            "ORDERS",
            ["orders.*"],
            TimeSpan.FromMinutes(2),
            TimeSpan.FromHours(1),
            4096,
            NatsStreamStorage.File,
            NatsStreamRetention.Limits,
            3
        );

        var config = NatsTopologyProvisioner.ToStreamConfig(stream);

        config.Name.Should().Be("ORDERS");
        config.Subjects.Should().Equal("orders.*");
        config.DuplicateWindow.Should().Be(TimeSpan.FromMinutes(2));
        config.MaxAge.Should().Be(TimeSpan.FromHours(1));
        config.MaxMsgSize.Should().Be(4096);
        config.NumReplicas.Should().Be(3);
    }

    [Fact]
    public void ToConsumerConfig_LeavesFilterSubjectUnsetWhenConsumerHasNoFilter()
    {
        NatsInboundConsumer consumer = new (
            "ORDERS",
            "orders-worker",
            null,
            1,
            TimeSpan.FromSeconds(30),
            5,
            1024,
            null,
            new Dictionary<string, NatsInboundEndpoint>(StringComparer.Ordinal)
        );

        var config = NatsTopologyProvisioner.ToConsumerConfig(consumer);

        config.FilterSubject.Should().BeNull();
        config.DeliverPolicy.Should().Be(ConsumerConfigDeliverPolicy.All);
        config.AckPolicy.Should().Be(ConsumerConfigAckPolicy.Explicit);
    }

    [Fact]
    public async Task AssertOnlyProvisioningSucceedsWhenJetStreamResourcesAlreadyExist()
    {
        await using var container = new NatsBuilder("nats:2.11-alpine")
           .WithCommand("-js")
           .Build();
        await container.StartAsync(TestContext.Current.CancellationToken);

        await using NatsConnection connection = new (new NatsOpts { Url = container.GetConnectionString() });
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
                MaxDeliver = 5,
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
                   .UseServer(container.GetConnectionString())
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
    public async Task AssertOnlyProvisioningFailsWhenJetStreamResourcesAreMissing()
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

        await act.Should().ThrowAsync<Exception>();
    }
}
