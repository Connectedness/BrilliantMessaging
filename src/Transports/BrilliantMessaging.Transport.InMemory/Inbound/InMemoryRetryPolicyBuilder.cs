using System;
using BrilliantMessaging.Core.Messaging;

namespace BrilliantMessaging.Transport.InMemory.Inbound;

/// <summary>
/// Fluent builder for a consumer's retry policy. It captures the maximum number of attempts and the backoff
/// strategy that spaces the retries.
/// </summary>
public sealed class InMemoryRetryPolicyBuilder : IBuildable<InMemoryRetryPolicy>
{
    private InMemoryBackoff _backoff = InMemoryBackoff.Immediate;
    private int _maxAttempts = 3;

    /// <inheritdoc />
    InMemoryRetryPolicy IBuildable<InMemoryRetryPolicy>.Build()
    {
        return new InMemoryRetryPolicy(_maxAttempts, _backoff);
    }

    /// <summary>
    /// Sets the maximum number of delivery attempts, counting the initial delivery. When the attempts are
    /// exhausted the delivery is dead-lettered or dropped.
    /// </summary>
    /// <param name="maxAttempts">The maximum number of attempts; must be greater than zero.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="maxAttempts" /> is less than one.</exception>
    public InMemoryRetryPolicyBuilder MaxAttempts(int maxAttempts)
    {
        if (maxAttempts < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxAttempts),
                maxAttempts,
                "The value must be greater than zero."
            );
        }

        _maxAttempts = maxAttempts;
        return this;
    }

    /// <summary>
    /// Uses linear backoff: the delay before the <c>n</c>-th retry is <paramref name="delay" /> multiplied by
    /// <c>n</c>.
    /// </summary>
    /// <param name="delay">The base delay; must be greater than zero.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="delay" /> is not greater than zero.</exception>
    public InMemoryRetryPolicyBuilder LinearBackoff(TimeSpan delay)
    {
        _backoff = new InMemoryBackoff(InMemoryBackoffKind.Linear, RequirePositive(delay));
        return this;
    }

    /// <summary>
    /// Uses exponential backoff: the delay before the <c>n</c>-th retry is <paramref name="delay" /> multiplied by
    /// <c>2^(n-1)</c>.
    /// </summary>
    /// <param name="delay">The base delay; must be greater than zero.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="delay" /> is not greater than zero.</exception>
    public InMemoryRetryPolicyBuilder ExponentialBackoff(TimeSpan delay)
    {
        _backoff = new InMemoryBackoff(InMemoryBackoffKind.Exponential, RequirePositive(delay));
        return this;
    }

    private static TimeSpan RequirePositive(TimeSpan delay)
    {
        if (delay <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(delay), delay, "The value must be greater than zero.");
        }

        return delay;
    }
}
