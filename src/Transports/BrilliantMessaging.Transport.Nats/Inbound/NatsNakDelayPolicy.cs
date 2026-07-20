using System;

namespace BrilliantMessaging.Transport.Nats.Inbound;

/// <summary>
/// Computes the delayed negative acknowledgement used for JetStream redelivery.
/// </summary>
public static class NatsNakDelayPolicy
{
    private const double MaximumBaseDelayMilliseconds = 5000;
    private const double MinimumBaseDelayMilliseconds = 100;

    private static readonly TimeSpan MaximumDelay = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Computes the delay before redelivery after the given delivery attempt fails.
    /// </summary>
    /// <param name="ackWait">The JetStream acknowledgement wait configured for the consumer.</param>
    /// <param name="deliveryAttempt">The one-based JetStream delivery attempt that failed.</param>
    /// <returns>The delay to send with the negative acknowledgement.</returns>
    public static TimeSpan GetDelay(TimeSpan ackWait, uint deliveryAttempt)
    {
        var baseMilliseconds = Math.Max(
            MinimumBaseDelayMilliseconds,
            Math.Min(ackWait.TotalMilliseconds / 2, MaximumBaseDelayMilliseconds)
        );
        var attempt = Math.Max(1, deliveryAttempt);
        var scaledMilliseconds = baseMilliseconds * Math.Pow(2, attempt - 1);
        var cappedMilliseconds = Math.Min(scaledMilliseconds, MaximumDelay.TotalMilliseconds);
        return TimeSpan.FromMilliseconds(cappedMilliseconds);
    }
}
