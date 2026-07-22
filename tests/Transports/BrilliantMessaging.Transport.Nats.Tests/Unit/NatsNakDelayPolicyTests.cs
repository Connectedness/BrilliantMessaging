using System;
using BrilliantMessaging.Transport.Nats.Inbound;
using FluentAssertions;
using Xunit;

namespace BrilliantMessaging.Transport.Nats.Tests.Unit;

public sealed class NatsNakDelayPolicyTests
{
    [Fact]
    public void GetDelay_DoublesFromHalfAckWaitForEachDeliveryAttempt()
    {
        var ackWait = TimeSpan.FromSeconds(3);
        TimeSpan[] delays =
        [
            NatsNakDelayPolicy.GetDelay(ackWait, 1),
            NatsNakDelayPolicy.GetDelay(ackWait, 2),
            NatsNakDelayPolicy.GetDelay(ackWait, 3),
            NatsNakDelayPolicy.GetDelay(ackWait, 4),
            NatsNakDelayPolicy.GetDelay(ackWait, 5)
        ];

        delays.Should()
           .Equal(
                TimeSpan.FromMilliseconds(1500),
                TimeSpan.FromSeconds(3),
                TimeSpan.FromSeconds(6),
                TimeSpan.FromSeconds(12),
                TimeSpan.FromSeconds(24)
            );
    }

    [Fact]
    public void GetDelay_ClampsTheBaseDelay()
    {
        NatsNakDelayPolicy.GetDelay(TimeSpan.FromMilliseconds(100), 1)
           .Should()
           .Be(TimeSpan.FromMilliseconds(100));
        NatsNakDelayPolicy.GetDelay(TimeSpan.FromSeconds(30), 1)
           .Should()
           .Be(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public void GetDelay_CapsTheScaledDelayAtThirtySeconds()
    {
        NatsNakDelayPolicy.GetDelay(TimeSpan.FromSeconds(30), 4)
           .Should()
           .Be(TimeSpan.FromSeconds(30));
        NatsNakDelayPolicy.GetDelay(TimeSpan.FromSeconds(30), 100)
           .Should()
           .Be(TimeSpan.FromSeconds(30));
    }

    [Fact]
    public void GetDelay_TreatsAttemptZeroAsTheFirstDeliveryAttempt()
    {
        NatsNakDelayPolicy.GetDelay(TimeSpan.FromSeconds(3), 0)
           .Should()
           .Be(TimeSpan.FromMilliseconds(1500));
    }
}
