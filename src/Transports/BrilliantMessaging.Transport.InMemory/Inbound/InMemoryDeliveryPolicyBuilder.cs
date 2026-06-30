using System;
using BrilliantMessaging.Core.Messaging;

namespace BrilliantMessaging.Transport.InMemory.Inbound;

/// <summary>
/// Fluent builder for a consumer's failure handling, configured through
/// <see cref="InMemoryInboundConsumerBuilder.OnFailure" />. Without any configuration the delivery is dropped;
/// <see cref="Retry" /> enables automatic retries and <see cref="DeadLetterTo" /> republishes exhausted or
/// rejected deliveries to another topic.
/// </summary>
public sealed class InMemoryDeliveryPolicyBuilder : IBuildable<InMemoryDeliveryPolicy>
{
    private string? _deadLetterTopic;
    private InMemoryRetryPolicy? _retryPolicy;

    /// <inheritdoc />
    InMemoryDeliveryPolicy IBuildable<InMemoryDeliveryPolicy>.Build()
    {
        return new InMemoryDeliveryPolicy(_retryPolicy, _deadLetterTopic);
    }

    /// <summary>
    /// Enables automatic retries with the configured maximum attempts and backoff. Ordinary handler failures and
    /// <see cref="Core.Messaging.Inbound.RetryMessageException" /> are retried; a
    /// <see cref="Core.Messaging.Inbound.RejectMessageException" /> is never retried.
    /// </summary>
    /// <param name="configure">A callback that configures the retry policy.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="configure" /> is <see langword="null" />.</exception>
    public InMemoryDeliveryPolicyBuilder Retry(Action<InMemoryRetryPolicyBuilder> configure)
    {
        if (configure is null)
        {
            throw new ArgumentNullException(nameof(configure));
        }

        InMemoryRetryPolicyBuilder builder = new ();
        configure(builder);
        _retryPolicy = ((IBuildable<InMemoryRetryPolicy>) builder).Build();
        return this;
    }

    /// <summary>
    /// Republishes a delivery to the named topic when its retries are exhausted, when it is rejected, or when it
    /// fails under the default drop policy. Without a dead-letter topic such deliveries are dropped.
    /// </summary>
    /// <param name="topic">The dead-letter topic name.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="topic" /> is null or whitespace.</exception>
    public InMemoryDeliveryPolicyBuilder DeadLetterTo(string topic)
    {
        if (string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(topic));
        }

        _deadLetterTopic = topic;
        return this;
    }
}
