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

        var act = async () => await scheduler.DelayAsync(TimeSpan.Zero, cancellation.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task RealTimeDelayScheduler_ObservesCancellationForPositiveDelay()
    {
        RealTimeInMemoryDelayScheduler scheduler = new ();
        using CancellationTokenSource cancellation = new ();
        await cancellation.CancelAsync();

        var act = async () => await scheduler.DelayAsync(TimeSpan.FromMinutes(1), cancellation.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
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
