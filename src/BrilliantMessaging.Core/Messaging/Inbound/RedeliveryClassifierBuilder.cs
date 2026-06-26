using System;
using BrilliantMessaging.Core.Messaging;

namespace BrilliantMessaging.Core.Messaging.Inbound;

/// <summary>
/// Fluent builder for a custom <see cref="RedeliveryClassifier" />.
/// </summary>
public sealed class RedeliveryClassifierBuilder : IBuildable<RedeliveryClassifier>
{
    private Func<Exception, bool> _shouldRetry = RedeliveryClassifier.RetryUnlessPoison.ShouldRetry;

    /// <inheritdoc />
    RedeliveryClassifier IBuildable<RedeliveryClassifier>.Build()
    {
        return new RedeliveryClassifier(_shouldRetry);
    }

    /// <summary>
    /// Sets the predicate used to decide whether a failure should be retried.
    /// </summary>
    /// <param name="shouldRetry">The predicate used when no marker exception overrides the decision.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="shouldRetry" /> is <see langword="null" />.</exception>
    public RedeliveryClassifierBuilder ShouldRetry(Func<Exception, bool> shouldRetry)
    {
        _shouldRetry = shouldRetry ?? throw new ArgumentNullException(nameof(shouldRetry));
        return this;
    }
}
