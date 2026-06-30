using System;
using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;
using BrilliantMessaging.Transport.InMemory.Inbound;
using BrilliantMessaging.Transport.InMemory.Outbound;
using BrilliantMessaging.Transport.InMemory.Tests.TestSupport;
using FluentAssertions;
using Xunit;

namespace BrilliantMessaging.Transport.InMemory.Tests;

public sealed class InMemoryIsolatedTests
{
    private static CancellationToken CancellationToken => TestContext.Current.CancellationToken;

    [Fact]
    public void Backoff_ReturnsZeroForImmediateAndUnknownKinds()
    {
        InMemoryBackoff.Immediate.GetDelay(1).Should().Be(TimeSpan.Zero);
        new InMemoryBackoff((InMemoryBackoffKind) 99, TimeSpan.FromSeconds(1))
           .GetDelay(1)
           .Should()
           .Be(TimeSpan.Zero);
    }

    [Fact]
    public void Backoff_ClampsOverflowToMaxValue()
    {
        InMemoryBackoff backoff = new (InMemoryBackoffKind.Exponential, TimeSpan.FromTicks(2));

        backoff.GetDelay(63).Should().Be(TimeSpan.MaxValue);
    }

    [Fact]
    public void Backoff_TreatsNonPositiveDelayOrAttemptAsZeroOrFirstRetry()
    {
        new InMemoryBackoff(InMemoryBackoffKind.Linear, TimeSpan.Zero)
           .GetDelay(3)
           .Should()
           .Be(TimeSpan.Zero);
        new InMemoryBackoff(InMemoryBackoffKind.Linear, TimeSpan.FromMilliseconds(5))
           .GetDelay(0)
           .Should()
           .Be(TimeSpan.FromMilliseconds(5));
    }

    [Fact]
    public async Task RealTimeDelayScheduler_CompletesImmediatelyForNonPositiveDelay()
    {
        RealTimeInMemoryDelayScheduler scheduler = new ();

        await scheduler.DelayAsync(TimeSpan.Zero, TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task RealTimeDelayScheduler_ReturnsCancelledTaskForNonPositiveDelayWhenTokenIsCancelled()
    {
        RealTimeInMemoryDelayScheduler scheduler = new ();
        using CancellationTokenSource cancellation = new ();
        await cancellation.CancelAsync();

        // ReSharper disable once AccessToDisposedClosure
        var act = async () => await scheduler.DelayAsync(TimeSpan.Zero, cancellation.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task RealTimeDelayScheduler_ObservesCancellationForPositiveDelay()
    {
        RealTimeInMemoryDelayScheduler scheduler = new ();
        using CancellationTokenSource cancellation = new ();
        await cancellation.CancelAsync();

        // ReSharper disable once AccessToDisposedClosure
        var act = async () => await scheduler.DelayAsync(TimeSpan.FromMinutes(1), cancellation.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public void ManualDelayScheduler_RecordsRequestedDelaysInOrder()
    {
        ManualDelayScheduler scheduler = new ();

        _ = scheduler.DelayAsync(TimeSpan.FromMilliseconds(10), CancellationToken.None);
        _ = scheduler.DelayAsync(TimeSpan.FromMilliseconds(20), CancellationToken.None);

        scheduler.RequestedDelays.Should().Equal(TimeSpan.FromMilliseconds(10), TimeSpan.FromMilliseconds(20));
        scheduler.PendingCount.Should().Be(2);
    }

    [Fact]
    public async Task ManualDelayScheduler_ReleaseAllCompletesPendingDelays()
    {
        ManualDelayScheduler scheduler = new ();
        var task = scheduler.DelayAsync(TimeSpan.FromMinutes(1), CancellationToken.None);
        scheduler.PendingCount.Should().Be(1);

        scheduler.ReleaseAll();

        await task;
        scheduler.PendingCount.Should().Be(0);
    }

    [Fact]
    public async Task ManualDelayScheduler_PropagatesCancellationForPendingDelay()
    {
        ManualDelayScheduler scheduler = new ();
        using CancellationTokenSource cancellation = new ();
        var task = scheduler.DelayAsync(TimeSpan.FromMinutes(1), cancellation.Token);

        await cancellation.CancelAsync();

        var act = async () => await task;
        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task ManualDelayScheduler_WaitForPendingCompletesWhenCountReached()
    {
        ManualDelayScheduler scheduler = new ();
        _ = scheduler.DelayAsync(TimeSpan.FromMinutes(1), CancellationToken.None);

        await scheduler.WaitForPendingAsync(1, TimeSpan.FromSeconds(5));

        scheduler.PendingCount.Should().Be(1);
    }

    [Fact]
    public async Task ManualDelayScheduler_WaitForPendingThrowsWhenCountNotReached()
    {
        ManualDelayScheduler scheduler = new ();

        var act = async () => await scheduler.WaitForPendingAsync(1, TimeSpan.FromMilliseconds(50));

        await act.Should().ThrowAsync<TimeoutException>();
    }

    [Fact]
    public async Task HandlerProbe_RecordsInvocationsInOrder()
    {
        HandlerProbe probe = new ();

        await probe.HandleAsync("orders", "endpoint", new OrderPlaced { OrderId = "a" }, 1, CancellationToken);
        await probe.HandleAsync("orders", "endpoint", new OrderPlaced { OrderId = "b" }, 2, CancellationToken);

        probe.Invocations.Should().HaveCount(2);
        probe.Invocations[0].Route.Should().Be("orders");
        probe.Invocations[0].DeliveryAttempt.Should().Be(1);
        probe.Invocations[1].DeliveryAttempt.Should().Be(2);
    }

    [Fact]
    public async Task HandlerProbe_OnHandleExceptionPropagates()
    {
        HandlerProbe probe = new ()
        {
            OnHandle = static _ => new InvalidOperationException("boom")
        };

        var act = async () => await probe.HandleAsync("orders", "endpoint", new OrderPlaced(), 1, CancellationToken);

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("boom");
    }

    [Fact]
    public async Task HandlerProbe_WaitForInvocationsCompletesAtThreshold()
    {
        HandlerProbe probe = new ();
        var wait = probe.WaitForInvocationsAsync(2, TimeSpan.FromSeconds(5));

        await probe.HandleAsync("orders", "endpoint", new OrderPlaced(), 1, CancellationToken);
        await probe.HandleAsync("orders", "endpoint", new OrderPlaced(), 2, CancellationToken);

        await wait;
    }

    [Fact]
    public async Task HandlerProbe_WaitForInvocationsThrowsBelowThreshold()
    {
        HandlerProbe probe = new ();

        var act = async () => await probe.WaitForInvocationsAsync(1, TimeSpan.FromMilliseconds(50));

        await act.Should().ThrowAsync<TimeoutException>();
    }

    [Fact]
    public async Task HandlerProbe_GateHoldsInvocationUntilCompleted()
    {
        HandlerProbe probe = new ();
        TaskCompletionSource<bool> gate = new (TaskCreationOptions.RunContinuationsAsynchronously);
        probe.Gate = gate;

        var handle = probe.HandleAsync("orders", "endpoint", new OrderPlaced(), 1, CancellationToken);

        // The invocation is recorded before the gate is awaited, so it is observable while still in flight.
        await probe.WaitForInvocationsAsync(1, TimeSpan.FromSeconds(5));
        handle.IsCompleted.Should().BeFalse();

        gate.SetResult(true);

        await handle;
    }

    [Fact]
    public async Task InMemoryTestHost_ProbeIsNullWhenNoneRegistered()
    {
        await using var host = await InMemoryTestHost.StartAsync(
            builder => builder
               .MapMessageContracts(static contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
               .AddInMemoryTopology(
                    topology => topology
                       .Topic("orders")
                       .Publish<OrderPlaced>(target => target.ToTopic("orders"))
                )
        );

        host.Probe.Should().BeNull();
    }

    [Fact]
    public async Task InMemoryTestHost_AppliesConfiguredCloudEventsSource()
    {
        await using var host = await InMemoryTestHost.StartAsync(
            builder => builder
               .MapMessageContracts(static contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
               .AddInMemoryTopology(
                    topology => topology
                       .Topic("orders")
                       .Publish<OrderPlaced>(target => target.ToTopic("orders"))
                ),
            source: "/custom-source"
        );

        await host.Publisher.PublishMessageAsync(
            new OrderPlaced { OrderId = "order-1" },
            cancellationToken: CancellationToken
        );
        await host.Broker.DrainUntilIdleAsync(TimeSpan.FromSeconds(5), CancellationToken);

        var recorded = host.Broker.GetMessages("orders").Should().ContainSingle().Which;
        var prefix = InMemoryOutboundTarget<OrderPlaced>.CloudEventsHeaderPrefix;
        recorded.Headers[$"{prefix}source"].Should().Be("/custom-source");
    }

    [Fact]
    public void InboundEndpoint_RejectsHandlerTypeThatDoesNotHandleMessage()
    {
        var act = () => new InMemoryInboundEndpoint<OrderPlaced>(
            "orders",
            Topology.DefaultName,
            typeof(string),
            typeof(PayloadCodecMessageDeserializer),
            "tests.order.placed",
            static _ => Task.CompletedTask,
            MessageAckMode.Auto,
            RedeliveryClassifier.RejectAll
        );

        act.Should().Throw<ArgumentException>().WithParameterName("handlerType");
    }

    [Fact]
    public void TopologyRuntime_RejectsNullTopology()
    {
        var act = () => new InMemoryTopologyRuntime(null!);

        act.Should().Throw<ArgumentNullException>().WithParameterName("topology");
    }

    [Fact]
    public void OutboundTarget_RejectsNullBroker()
    {
        var registryBuilder = new MessageContractRegistryBuilder();
        registryBuilder.Map<OrderPlaced>("tests.order.placed");

        var act = () => new InMemoryOutboundTarget<OrderPlaced>(
            "orders",
            new FixedEnvelopeMessageSerializer(CreateEnvelope()),
            ((IBuildable<IMessageContractRegistry>) registryBuilder).Build(),
            Topology.DefaultName,
            "orders",
            null!
        );

        act.Should().Throw<ArgumentNullException>().WithParameterName("broker");
    }

    private static CloudEventEnvelope CreateEnvelope()
    {
        return new CloudEventEnvelope(
            "1.0",
            "message-id",
            "/tests",
            "tests.order.placed",
            DateTimeOffset.UtcNow,
            null,
            "application/json",
            null,
            "{}"u8.ToArray()
        );
    }
}
