using System;

namespace Usf.Transport.RabbitMq.Configuration;

public static class RabbitMqPublisherConfirmDefaults
{
    /// <summary>
    /// Gets the publisher confirm mode applied when neither a channel group nor the topology overrides it.
    /// Publisher confirmations are on by default so a broker NACK or an unroutable mandatory message cannot
    /// be lost silently; opt out explicitly with <see cref="RabbitMqPublisherConfirmMode.FireAndForget" />.
    /// </summary>
    public const RabbitMqPublisherConfirmMode Mode = RabbitMqPublisherConfirmMode.Confirms;

    /// <summary>
    /// Gets the bounded wait applied to publisher confirmations when no override is configured.
    /// </summary>
    public static TimeSpan Timeout { get; } = TimeSpan.FromSeconds(30);

    internal static bool IsValidTimeout(TimeSpan timeout)
    {
        // CancellationTokenSource schedules its delay on a Timer, which rejects intervals above
        // (uint.MaxValue - 1) milliseconds (~49.7 days). A confirm timeout must stay within that bound so it
        // can be used as the CancellationTokenSource delay that enforces the bounded wait.
        return timeout > TimeSpan.Zero && timeout <= TimeSpan.FromMilliseconds(uint.MaxValue - 1d);
    }
}
