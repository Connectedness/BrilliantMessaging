using System;

namespace Usf.Transport.RabbitMq.Configuration;

public static class RabbitMqPublisherConfirmDefaults
{
    /// <summary>
    /// Gets the bounded wait applied to publisher confirmations when no override is configured.
    /// </summary>
    public static TimeSpan Timeout { get; } = TimeSpan.FromSeconds(30);

    internal static bool IsValidTimeout(TimeSpan timeout)
    {
        return timeout > TimeSpan.Zero && timeout <= TimeSpan.FromMilliseconds(uint.MaxValue - 1d);
    }
}
