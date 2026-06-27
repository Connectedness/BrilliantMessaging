using System;

namespace BrilliantMessaging.Core.Messaging.Inbound;

/// <summary>
/// Classifies inbound handler failures as retryable or poison for broker redelivery.
/// </summary>
/// <remarks>
/// The classifier only answers whether the current failure should be requeued. It does not count attempts,
/// schedule delays, or choose an exhaustion action.
/// </remarks>
public sealed class RedeliveryClassifier
{
    private readonly bool _retryMarkerOverrides;
    private readonly Func<Exception, bool> _shouldRetry;

    /// <summary>
    /// Initializes a new instance of the <see cref="RedeliveryClassifier" /> class.
    /// </summary>
    /// <param name="shouldRetry">The predicate used when no marker exception overrides the decision.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="shouldRetry" /> is <see langword="null" />.</exception>
    public RedeliveryClassifier(Func<Exception, bool> shouldRetry)
        : this(shouldRetry, retryMarkerOverrides: true) { }

    private RedeliveryClassifier(Func<Exception, bool> shouldRetry, bool retryMarkerOverrides)
    {
        _shouldRetry = shouldRetry ?? throw new ArgumentNullException(nameof(shouldRetry));
        _retryMarkerOverrides = retryMarkerOverrides;
    }

    /// <summary>
    /// Gets a classifier that retries every failure except <see cref="MessageDeserializationException" /> and
    /// explicit <see cref="RejectMessageException" /> failures.
    /// </summary>
    public static RedeliveryClassifier RetryUnlessPoison { get; } =
        new (static failure => failure is not MessageDeserializationException);

    /// <summary>
    /// Gets a classifier that rejects every failure without requeueing.
    /// </summary>
    public static RedeliveryClassifier RejectAll { get; } =
        new (static _ => false, retryMarkerOverrides: false);

    /// <summary>
    /// Returns whether the supplied failure should be retried through broker redelivery.
    /// </summary>
    /// <param name="failure">The exception thrown while processing the inbound message.</param>
    /// <returns><see langword="true" /> when the message should be settled with requeue; otherwise <see langword="false" />.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="failure" /> is <see langword="null" />.</exception>
    public bool ShouldRetry(Exception failure)
    {
        if (failure is null)
        {
            throw new ArgumentNullException(nameof(failure));
        }

        if (failure is RejectMessageException)
        {
            return false;
        }

        if (_retryMarkerOverrides && failure is RetryMessageException)
        {
            return true;
        }

        return _shouldRetry(failure);
    }
}
