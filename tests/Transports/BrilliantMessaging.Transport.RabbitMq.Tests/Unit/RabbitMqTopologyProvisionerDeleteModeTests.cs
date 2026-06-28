using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;
using BrilliantMessaging.Core.Messaging.Outbound;
using BrilliantMessaging.Transport.RabbitMq.Inbound;
using BrilliantMessaging.Transport.RabbitMq.Tests.TestSupport;
using FluentAssertions;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using Xunit;

namespace BrilliantMessaging.Transport.RabbitMq.Tests.Unit;

public sealed class RabbitMqTopologyProvisionerDeleteModeTests
{
    [Fact]
    public async Task ProvisionAsync_ThrowsForExchangeDeleteMode()
    {
        RabbitMqExchangeDefinition exchange = new (
            "bad-exchange",
            ExchangeType.Direct,
            RabbitMqDeclareMode.Delete,
            true,
            false,
            new Dictionary<string, object?>()
        );
        var topology = BuildMinimalTopology(exchanges: [exchange]);

        var provisioner = new RabbitMqTopologyProvisioner(topology);
        var act = async () => await provisioner.ProvisionAsync(CancellationToken.None);

        (await act.Should().ThrowAsync<ArgumentOutOfRangeException>())
           .Which.Message.Should().Contain("Exchange deletion is not supported");
    }

    [Fact]
    public async Task ProvisionAsync_ThrowsForExchangeBindingDeleteMode()
    {
        RabbitMqExchangeDefinition sourceExchange = new (
            "source",
            ExchangeType.Direct,
            RabbitMqDeclareMode.Active,
            true,
            false,
            new Dictionary<string, object?>()
        );
        RabbitMqExchangeDefinition destinationExchange = new (
            "destination",
            ExchangeType.Direct,
            RabbitMqDeclareMode.Active,
            true,
            false,
            new Dictionary<string, object?>()
        );
        RabbitMqExchangeBindingDefinition exchangeBinding = new (
            "source",
            "destination",
            "routing",
            RabbitMqBindingMode.Delete,
            new Dictionary<string, object?>()
        );
        var topology = BuildMinimalTopology(
            exchanges: [sourceExchange, destinationExchange],
            bindings: [exchangeBinding]
        );

        var provisioner = new RabbitMqTopologyProvisioner(topology);
        var act = async () => await provisioner.ProvisionAsync(CancellationToken.None);

        (await act.Should().ThrowAsync<ArgumentOutOfRangeException>())
           .Which.Message.Should().Contain("Exchange binding deletion is not supported");
    }

    [Fact]
    public async Task ProvisionAsync_SkipsQueueInSkipMode()
    {
        RabbitMqQueueDefinition queue = new (
            "skip-queue",
            RabbitMqDeclareMode.Skip,
            true,
            false,
            false,
            new Dictionary<string, object?>()
        );
        var topology = BuildMinimalTopology(queues: [queue]);

        var provisioner = new RabbitMqTopologyProvisioner(topology);
        var act = async () => await provisioner.ProvisionAsync(CancellationToken.None);

        await act.Should().NotThrowAsync<Exception>();
    }

    [Fact]
    public async Task ProvisionAsync_PassivelyDeclaresQueueInPassiveMode()
    {
        RabbitMqQueueDefinition queue = new (
            "passive-queue",
            RabbitMqDeclareMode.Passive,
            true,
            false,
            false,
            new Dictionary<string, object?>()
        );
        var topology = BuildMinimalTopology(queues: [queue]);

        var provisioner = new RabbitMqTopologyProvisioner(topology);
        var act = async () => await provisioner.ProvisionAsync(CancellationToken.None);

        // The test channel's QueueDeclarePassiveAsync returns the default (null Task or default value),
        // so this should not throw. The dispatch proxy handles it by returning the default value.
        await act.Should().NotThrowAsync<Exception>();
    }

    [Fact]
    public async Task ProvisionAsync_DeletesEmptyQueueInDeleteMode()
    {
        TestRabbitMqChannel testChannel = new ()
        {
            QueueDeclarePassiveAsyncHandler = (name, _) => Task.FromResult(new QueueDeclareOk(name, 0, 0)),
            QueueDeleteAsyncHandler = (_, _) => Task.FromResult(0u)
        };
        var topology = BuildMinimalTopology(queues: [DeleteQueue("delete-queue")], channel: testChannel);

        var provisioner = new RabbitMqTopologyProvisioner(topology);
        var act = async () => await provisioner.ProvisionAsync(CancellationToken.None);

        await act.Should().NotThrowAsync<Exception>();
        testChannel.QueueDeleteCallCount.Should().Be(1);
    }

    [Fact]
    public async Task ProvisionAsync_ThrowsWhenQueueInDeleteModeStillHasMessages()
    {
        TestRabbitMqChannel testChannel = new ()
        {
            QueueDeclarePassiveAsyncHandler = (name, _) => Task.FromResult(new QueueDeclareOk(name, 3, 0))
        };
        var topology = BuildMinimalTopology(queues: [DeleteQueue("non-empty-queue")], channel: testChannel);

        var provisioner = new RabbitMqTopologyProvisioner(topology);
        var act = async () => await provisioner.ProvisionAsync(CancellationToken.None);

        (await act.Should().ThrowAsync<InvalidOperationException>())
           .Which.Message.Should().Contain("still has 3 message(s)");
        testChannel.QueueDeleteCallCount.Should().Be(0);
    }

    [Fact]
    public async Task ProvisionAsync_TreatsNotFoundOnPassiveDeclareAsSuccess()
    {
        TestRabbitMqChannel testChannel = new ()
        {
            QueueDeclarePassiveAsyncHandler = (_, _) => Task.FromException<QueueDeclareOk>(CreateNotFound())
        };
        var topology = BuildMinimalTopology(queues: [DeleteQueue("absent-queue")], channel: testChannel);

        var provisioner = new RabbitMqTopologyProvisioner(topology);
        var act = async () => await provisioner.ProvisionAsync(CancellationToken.None);

        await act.Should().NotThrowAsync<Exception>();
        testChannel.QueueDeleteCallCount.Should().Be(0);
    }

    [Fact]
    public async Task ProvisionAsync_TreatsNotFoundOnDeleteAsSuccess()
    {
        // The queue is present and empty at the passive declare, then removed (operator action, concurrent
        // deploy) before the delete call lands. The resulting 404 must be treated as success so Delete mode
        // stays idempotent rather than failing provisioning.
        TestRabbitMqChannel testChannel = new ()
        {
            QueueDeclarePassiveAsyncHandler = (name, _) => Task.FromResult(new QueueDeclareOk(name, 0, 0)),
            QueueDeleteAsyncHandler = (_, _) => Task.FromException<uint>(CreateNotFound())
        };
        var topology = BuildMinimalTopology(queues: [DeleteQueue("racing-queue")], channel: testChannel);

        var provisioner = new RabbitMqTopologyProvisioner(topology);
        var act = async () => await provisioner.ProvisionAsync(CancellationToken.None);

        await act.Should().NotThrowAsync<Exception>();
        testChannel.QueueDeleteCallCount.Should().Be(1);
    }

    [Fact]
    public async Task ProvisionAsync_RenewsChannelAfterDeleteClosesItAndProvisionsRemainingResources()
    {
        // A Delete-mode queue that is already absent makes the broker close the channel with a NOT_FOUND.
        // The provisioner must acquire a fresh channel so the following Active queue is still declared,
        // rather than running on the dead channel. Without renewal the second declare would either land on
        // closedChannel or fail outright.
        TestRabbitMqChannel closedChannel = new ();
        closedChannel.QueueDeclarePassiveAsyncHandler = (_, _) =>
        {
            // Model the broker closing the channel as part of raising the NOT_FOUND.
            closedChannel.Close(Constants.NotFound, "NOT_FOUND");
            return Task.FromException<QueueDeclareOk>(CreateNotFound());
        };
        TestRabbitMqChannel renewedChannel = new ();

        var topology = BuildMinimalTopology(
            queues: [DeleteQueue("absent-queue"), ActiveQueue("new-queue")],
            channel: closedChannel,
            additionalChannels: [renewedChannel]
        );

        var provisioner = new RabbitMqTopologyProvisioner(topology);
        var act = async () => await provisioner.ProvisionAsync(CancellationToken.None);

        await act.Should().NotThrowAsync<Exception>();
        closedChannel.IsOpen.Should().BeFalse();
        closedChannel.QueueDeclareCallCount.Should().Be(0);
        renewedChannel.QueueDeclareCallCount.Should().Be(1);
    }

    private static RabbitMqQueueDefinition ActiveQueue(string name)
    {
        return new RabbitMqQueueDefinition(
            name,
            RabbitMqDeclareMode.Active,
            true,
            false,
            false,
            new Dictionary<string, object?>()
        );
    }

    private static RabbitMqQueueDefinition DeleteQueue(string name)
    {
        return new RabbitMqQueueDefinition(
            name,
            RabbitMqDeclareMode.Delete,
            true,
            false,
            false,
            new Dictionary<string, object?>()
        );
    }

    private static OperationInterruptedException CreateNotFound()
    {
        return new OperationInterruptedException(
            new ShutdownEventArgs(ShutdownInitiator.Peer, Constants.NotFound, "NOT_FOUND")
        );
    }

    private static RabbitMqTopology BuildMinimalTopology(
        IReadOnlyList<RabbitMqExchangeDefinition>? exchanges = null,
        IReadOnlyList<RabbitMqQueueDefinition>? queues = null,
        IReadOnlyList<RabbitMqBindingDefinition>? bindings = null,
        TestRabbitMqChannel? channel = null,
        IReadOnlyList<TestRabbitMqChannel>? additionalChannels = null
    )
    {
        var testChannel = channel ?? new TestRabbitMqChannel();
        TestRabbitMqConnection testConnection = new ();
        testConnection.EnqueueChannel(testChannel.Object);

        // Additional channels back the pool's renewal path: when the broker closes a channel (a swallowed
        // NOT_FOUND), the provisioner acquires a fresh one, which the connection dispenses from this queue.
        if (additionalChannels is not null)
        {
            foreach (var renewalChannel in additionalChannels)
            {
                testConnection.EnqueueChannel(renewalChannel.Object);
            }
        }

        RabbitMqConnectionProvider connectionProvider = new (
            _ => Task.FromResult(testConnection.Object)
        );
        RabbitMqChannelSource channelSource = new (connectionProvider);
        channelSource.SetChannelBudget(0, "no channel groups configured");

        return new RabbitMqTopology(
            "test",
            TopologyData.PrepareTopologyDataStructures(
                new Dictionary<Type, OutboundTarget>(),
                new Dictionary<string, OutboundTarget>(StringComparer.Ordinal),
                new Dictionary<string, InboundEndpoint>(StringComparer.Ordinal)
            ),
            RabbitMqCloudEventsTestFactory.CreateRegistry(),
            exchanges ?? [],
            queues ?? [],
            bindings ?? [],
            [],
            [],
            [],
            [],
            [],
            new Dictionary<InboundEndpointSelectionKey, RabbitMqInboundEndpoint>(),
            static _ => Task.CompletedTask,
            TimeSpan.FromSeconds(1),
            connectionProvider,
            channelSource
        );
    }
}
