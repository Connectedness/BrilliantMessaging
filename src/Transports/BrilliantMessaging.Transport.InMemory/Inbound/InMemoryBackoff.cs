using System;

namespace BrilliantMessaging.Transport.InMemory.Inbound;

/// <summary>
/// Identifies how the delay between retry attempts grows.
/// </summary>
public enum InMemoryBackoffKind
{
    /// <summary>
    /// No backoff: each retry is scheduled immediately.
    /// </summary>
    None = 0,

    /// <summary>
    /// Linear backoff: the delay before the <c>n</c>-th retry is the base delay multiplied by <c>n</c>.
    /// </summary>
    Linear = 1,

    /// <summary>
    /// Exponential backoff: the delay before the <c>n</c>-th retry is the base delay multiplied by
    /// <c>2^(n-1)</c>.
    /// </summary>
    Exponential = 2
}

/// <summary>
/// The compiled backoff strategy for a retry policy: a <see cref="InMemoryBackoffKind" /> and the base delay it
/// scales.
/// </summary>
/// <param name="Kind">The backoff growth strategy.</param>
/// <param name="Delay">The base delay the strategy scales; ignored for <see cref="InMemoryBackoffKind.None" />.</param>
public readonly record struct InMemoryBackoff(InMemoryBackoffKind Kind, TimeSpan Delay)
{
    /// <summary>
    /// Gets the immediate backoff that schedules every retry without delay.
    /// </summary>
    public static InMemoryBackoff Immediate => new (InMemoryBackoffKind.None, TimeSpan.Zero);

    /// <summary>
    /// Computes the delay before the retry that follows the given failed attempt.
    /// </summary>
    /// <param name="failedAttempt">The one-based attempt number that just failed (the first delivery is attempt 1).</param>
    /// <returns>The delay to wait before the next attempt.</returns>
    public TimeSpan GetDelay(int failedAttempt)
    {
        // The retry index is the number of retries that have happened so far, counting the one being scheduled:
        // attempt 1 failing schedules retry index 1, attempt 2 failing schedules retry index 2, and so on.
        var retryIndex = failedAttempt < 1 ? 1 : failedAttempt;

        return Kind switch
        {
            InMemoryBackoffKind.None => TimeSpan.Zero,
            InMemoryBackoffKind.Linear => ScaleTicks(Delay, retryIndex),
            InMemoryBackoffKind.Exponential => ScaleTicks(Delay, 1L << (retryIndex - 1)),
            _ => TimeSpan.Zero
        };
    }

    private static TimeSpan ScaleTicks(TimeSpan delay, long multiplier)
    {
        // Guard against overflow when a deep exponential retry chain would exceed TimeSpan's range.
        if (delay <= TimeSpan.Zero || multiplier <= 0)
        {
            return TimeSpan.Zero;
        }

        if (multiplier > TimeSpan.MaxValue.Ticks / delay.Ticks)
        {
            return TimeSpan.MaxValue;
        }

        return TimeSpan.FromTicks(delay.Ticks * multiplier);
    }
}
