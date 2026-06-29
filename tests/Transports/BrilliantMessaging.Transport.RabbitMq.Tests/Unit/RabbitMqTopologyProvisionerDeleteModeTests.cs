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
    public async Task ProvisionAsync_DeletesExchangeInDeleteMode()
    {
        TestRabbitMqChannel testChannel = new ();
        var topology = BuildMinimalTopology(exchanges: [DeleteExchange("delete-exchange")], channel: testChannel);

        var provisioner = new RabbitMqTopologyProvisioner(topology);
        var act = async () => await provisioner.ProvisionAsync(CancellationToken.None);

        await act.Should().NotThrowAsync<Exception>();
        testChannel.ExchangeDeleteCallCount.Should().Be(1);
    }

    [Fact]
    public async Task ProvisionAsync_TreatsNotFoundOnExchangeDeleteAsSuccess()
    {
        // An already-absent exchange makes the broker close the channel with a NOT_FOUND on ExchangeDeleteAsync.
        // The resulting 404 must be treated as success so Delete mode stays idempotent across restarts.
        TestRabbitMqChannel testChannel = new ()
        {
            ExchangeDeleteAsyncHandler = (_, _) => Task.FromException(CreateNotFound())
        };
        var topology = BuildMinimalTopology(exchanges: [DeleteExchange("absent-exchange")], channel: testChannel);

        var provisioner = new RabbitMqTopologyProvisioner(topology);
        var act = async () => await provisioner.ProvisionAsync(CancellationToken.None);

        await act.Should().NotThrowAsync<Exception>();
        testChannel.ExchangeDeleteCallCount.Should().Be(1);
    }

    [Fact]
    public async Task ProvisionAsync_UnbindsExchangeBindingInDeleteMode()
    {
        TestRabbitMqChannel testChannel = new ();
        RabbitMqExchangeBindingDefinition exchangeBinding = new (
            "source",
            "destination",
            "routing",
            RabbitMqBindingMode.Delete,
            new Dictionary<string, object?>()
        );
        var topology = BuildMinimalTopology(
            exchanges:
            [
                ActiveExchange("source", ExchangeType.Direct), ActiveExchange("destination", ExchangeType.Direct)
            ],
            bindings: [exchangeBinding],
            channel: testChannel
        );

        var provisioner = new RabbitMqTopologyProvisioner(topology);
        var act = async () => await provisioner.ProvisionAsync(CancellationToken.None);

        await act.Should().NotThrowAsync<Exception>();
        testChannel.ExchangeUnbindCallCount.Should().Be(1);
    }

    [Fact]
    public async Task ProvisionAsync_SkipsQueueBindingReferencingDeleteExchangeAsSource()
    {
        // A binding whose source exchange is in Delete mode is skipped entirely: the exchange phase already
        // deleted the exchange and the broker cascade-removed its bindings, so a re-bind would target an absent
        // exchange and an unbind would be redundant. The binding here is Active, proving the skip happens
        // regardless of the binding's own mode.
        TestRabbitMqChannel testChannel = new ();
        RabbitMqQueueBindingDefinition queueBinding = new (
            "doomed-source",
            "queue",
            "routing",
            RabbitMqBindingMode.Active,
            new Dictionary<string, object?>()
        );
        var topology = BuildMinimalTopology(
            exchanges: [DeleteExchange("doomed-source")],
            queues: [ActiveQueue("queue")],
            bindings: [queueBinding],
            channel: testChannel
        );

        var provisioner = new RabbitMqTopologyProvisioner(topology);
        var act = async () => await provisioner.ProvisionAsync(CancellationToken.None);

        await act.Should().NotThrowAsync<Exception>();
        // The binding is skipped, so no bind/unbind call lands on the channel beyond the exchange delete.
        testChannel.ExchangeDeleteCallCount.Should().Be(1);
    }

    [Fact]
    public async Task ProvisionAsync_SkipsExchangeBindingReferencingDeleteExchangeAsDestination()
    {
        // An exchange binding whose destination is in Delete mode is skipped entirely, mirroring the source-end
        // skip: the exchange phase already deleted the destination and the broker cascade-removed its bindings.
        TestRabbitMqChannel testChannel = new ();
        RabbitMqExchangeBindingDefinition exchangeBinding = new (
            "source",
            "doomed-destination",
            "routing",
            RabbitMqBindingMode.Active,
            new Dictionary<string, object?>()
        );
        var topology = BuildMinimalTopology(
            exchanges: [ActiveExchange("source", ExchangeType.Direct), DeleteExchange("doomed-destination")],
            bindings: [exchangeBinding],
            channel: testChannel
        );

        var provisioner = new RabbitMqTopologyProvisioner(topology);
        var act = async () => await provisioner.ProvisionAsync(CancellationToken.None);

        await act.Should().NotThrowAsync<Exception>();
        testChannel.ExchangeDeleteCallCount.Should().Be(1);
        testChannel.ExchangeUnbindCallCount.Should().Be(0);
    }

    [Fact]
    public async Task ProvisionAsync_RenewsChannelAfterExchangeDeleteClosesItAndProvisionsRemainingResources()
    {
        // A Delete-mode exchange that is already absent makes the broker close the channel with a NOT_FOUND.
        // The provisioner must acquire a fresh channel so the following Active exchange is still declared,
        // rather than running on the dead channel. Mirrors the queue-delete renewal test.
        TestRabbitMqChannel closedChannel = new ();
        closedChannel.ExchangeDeleteAsyncHandler = (_, _) =>
        {
            closedChannel.Close(Constants.NotFound, "NOT_FOUND");
            return Task.FromException(CreateNotFound());
        };
        TestRabbitMqChannel renewedChannel = new ();

        var topology = BuildMinimalTopology(
            exchanges: [DeleteExchange("absent-exchange"), ActiveExchange("new-exchange", ExchangeType.Direct)],
            channel: closedChannel,
            additionalChannels: [renewedChannel]
        );

        var provisioner = new RabbitMqTopologyProvisioner(topology);
        var act = async () => await provisioner.ProvisionAsync(CancellationToken.None);

        await act.Should().NotThrowAsync<Exception>();
        closedChannel.IsOpen.Should().BeFalse();
        closedChannel.ExchangeDeleteCallCount.Should().Be(1);
        renewedChannel.ExchangeDeclareCallCount.Should().Be(1);
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

    [Fact]
    public async Task ProvisionAsync_TreatsNotFoundOnExchangeBindingUnbindAsSuccess()
    {
        // An exchange binding in Delete mode whose source or destination exchange is already absent makes the
        // broker close the channel with a NOT_FOUND on ExchangeUnbindAsync. The 404 must be treated as success
        // so Delete mode stays idempotent.
        TestRabbitMqChannel testChannel = new ()
        {
            ExchangeUnbindAsyncHandler = () => Task.FromException(CreateNotFound())
        };
        RabbitMqExchangeBindingDefinition exchangeBinding = new (
            "source",
            "destination",
            "routing",
            RabbitMqBindingMode.Delete,
            new Dictionary<string, object?>()
        );
        var topology = BuildMinimalTopology(
            exchanges:
            [
                ActiveExchange("source", ExchangeType.Direct), ActiveExchange("destination", ExchangeType.Direct)
            ],
            bindings: [exchangeBinding],
            channel: testChannel
        );

        var provisioner = new RabbitMqTopologyProvisioner(topology);
        var act = async () => await provisioner.ProvisionAsync(CancellationToken.None);

        await act.Should().NotThrowAsync<Exception>();
        testChannel.ExchangeUnbindCallCount.Should().Be(1);
    }

    [Fact]
    public async Task ProvisionAsync_TreatsNotFoundOnQueueBindingUnbindAsSuccess()
    {
        // A queue binding in Delete mode whose queue or exchange is already absent makes the broker close the
        // channel with a NOT_FOUND on QueueUnbindAsync. The 404 must be treated as success.
        TestRabbitMqChannel testChannel = new ()
        {
            QueueUnbindAsyncHandler = () => Task.FromException(CreateNotFound())
        };
        RabbitMqQueueBindingDefinition queueBinding = new (
            "source",
            "queue",
            "routing",
            RabbitMqBindingMode.Delete,
            new Dictionary<string, object?>()
        );
        var topology = BuildMinimalTopology(
            exchanges: [ActiveExchange("source", ExchangeType.Direct)],
            queues: [ActiveQueue("queue")],
            bindings: [queueBinding],
            channel: testChannel
        );

        var provisioner = new RabbitMqTopologyProvisioner(topology);
        var act = async () => await provisioner.ProvisionAsync(CancellationToken.None);

        await act.Should().NotThrowAsync<Exception>();
        testChannel.QueueUnbindCallCount.Should().Be(1);
    }

    [Fact]
    public async Task ProvisionAsync_SkipsExchangeInSkipMode()
    {
        RabbitMqExchangeDefinition exchange = new (
            "skip-exchange",
            ExchangeType.Direct,
            RabbitMqDeclareMode.Skip,
            true,
            false,
            new Dictionary<string, object?>()
        );
        var topology = BuildMinimalTopology(exchanges: [exchange]);

        var provisioner = new RabbitMqTopologyProvisioner(topology);
        var act = async () => await provisioner.ProvisionAsync(CancellationToken.None);

        await act.Should().NotThrowAsync<Exception>();
    }

    [Fact]
    public async Task ProvisionAsync_SkipsQueueBindingInSkipMode()
    {
        TestRabbitMqChannel testChannel = new ();
        RabbitMqQueueBindingDefinition queueBinding = new (
            "source",
            "queue",
            "routing",
            RabbitMqBindingMode.Skip,
            new Dictionary<string, object?>()
        );
        var topology = BuildMinimalTopology(
            exchanges: [ActiveExchange("source", ExchangeType.Direct)],
            queues: [ActiveQueue("queue")],
            bindings: [queueBinding],
            channel: testChannel
        );

        var provisioner = new RabbitMqTopologyProvisioner(topology);
        var act = async () => await provisioner.ProvisionAsync(CancellationToken.None);

        await act.Should().NotThrowAsync<Exception>();
        testChannel.QueueUnbindCallCount.Should().Be(0);
    }

    [Fact]
    public async Task ProvisionAsync_SkipsExchangeBindingInSkipMode()
    {
        TestRabbitMqChannel testChannel = new ();
        RabbitMqExchangeBindingDefinition exchangeBinding = new (
            "source",
            "destination",
            "routing",
            RabbitMqBindingMode.Skip,
            new Dictionary<string, object?>()
        );
        var topology = BuildMinimalTopology(
            exchanges:
            [
                ActiveExchange("source", ExchangeType.Direct), ActiveExchange("destination", ExchangeType.Direct)
            ],
            bindings: [exchangeBinding],
            channel: testChannel
        );

        var provisioner = new RabbitMqTopologyProvisioner(topology);
        var act = async () => await provisioner.ProvisionAsync(CancellationToken.None);

        await act.Should().NotThrowAsync<Exception>();
        testChannel.ExchangeUnbindCallCount.Should().Be(0);
    }

    [Fact]
    public async Task ProvisionAsync_ThrowsForUnsupportedExchangeDeclareMode()
    {
        RabbitMqExchangeDefinition exchange = new (
            "bad-exchange",
            ExchangeType.Direct,
            (RabbitMqDeclareMode) 999,
            true,
            false,
            new Dictionary<string, object?>()
        );
        var topology = BuildMinimalTopology(exchanges: [exchange]);

        var provisioner = new RabbitMqTopologyProvisioner(topology);
        var act = async () => await provisioner.ProvisionAsync(CancellationToken.None);

        (await act.Should().ThrowAsync<ArgumentOutOfRangeException>())
           .Which.Message.Should().Contain("Unsupported declare mode");
    }

    [Fact]
    public async Task ProvisionAsync_ThrowsForUnsupportedQueueDeclareMode()
    {
        RabbitMqQueueDefinition queue = new (
            "bad-queue",
            (RabbitMqDeclareMode) 999,
            true,
            false,
            false,
            new Dictionary<string, object?>()
        );
        var topology = BuildMinimalTopology(queues: [queue]);

        var provisioner = new RabbitMqTopologyProvisioner(topology);
        var act = async () => await provisioner.ProvisionAsync(CancellationToken.None);

        (await act.Should().ThrowAsync<ArgumentOutOfRangeException>())
           .Which.Message.Should().Contain("Unsupported declare mode");
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

    private static RabbitMqExchangeDefinition ActiveExchange(string name, string type)
    {
        return new RabbitMqExchangeDefinition(
            name,
            type,
            RabbitMqDeclareMode.Active,
            true,
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

    private static RabbitMqExchangeDefinition DeleteExchange(string name)
    {
        return new RabbitMqExchangeDefinition(
            name,
            ExchangeType.Direct,
            RabbitMqDeclareMode.Delete,
            true,
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
