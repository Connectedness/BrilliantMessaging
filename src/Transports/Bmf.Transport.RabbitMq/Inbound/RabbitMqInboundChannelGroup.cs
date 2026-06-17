using System;
using RabbitMQ.Client;

namespace Bmf.Transport.RabbitMq.Inbound;

/// <summary>
/// A group of consumer channels that share a prefetch count and consumer dispatch concurrency. Consumers bound
/// to the group are spread across up to its maximum channel count.
/// </summary>
public sealed class RabbitMqInboundChannelGroup
{
    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMqInboundChannelGroup" /> class.
    /// </summary>
    /// <param name="name">The group name.</param>
    /// <param name="maximumChannelCount">The maximum number of consumer channels the group may open; must be greater than zero.</param>
    /// <param name="prefetchCount">The per-consumer prefetch (QoS) count; must be greater than zero.</param>
    /// <param name="consumerDispatchConcurrency">The consumer dispatch concurrency per channel; must be greater than zero.</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="name" /> is null or whitespace.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when any numeric argument is out of range.</exception>
    public RabbitMqInboundChannelGroup(
        string name,
        int maximumChannelCount,
        ushort prefetchCount,
        ushort consumerDispatchConcurrency
    )
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(name));
        }

        if (maximumChannelCount < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maximumChannelCount),
                maximumChannelCount,
                "The value must be greater than zero."
            );
        }

        if (prefetchCount == 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(prefetchCount),
                prefetchCount,
                "The value must be greater than zero."
            );
        }

        if (consumerDispatchConcurrency == 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(consumerDispatchConcurrency),
                consumerDispatchConcurrency,
                "The value must be greater than zero."
            );
        }

        Name = name;
        MaximumChannelCount = maximumChannelCount;
        PrefetchCount = prefetchCount;
        ConsumerDispatchConcurrency = consumerDispatchConcurrency;
    }

    /// <summary>
    /// Gets the group name.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Gets the maximum number of consumer channels the group may open.
    /// </summary>
    public int MaximumChannelCount { get; }

    /// <summary>
    /// Gets the per-consumer prefetch (QoS) count.
    /// </summary>
    public ushort PrefetchCount { get; }

    /// <summary>
    /// Gets the consumer dispatch concurrency per channel.
    /// </summary>
    public ushort ConsumerDispatchConcurrency { get; }

    /// <summary>
    /// Builds the <see cref="CreateChannelOptions" /> for a consumer channel in this group, with publisher
    /// confirmations disabled and the group's dispatch concurrency applied.
    /// </summary>
    /// <returns>The channel options.</returns>
    public CreateChannelOptions CreateChannelOptions()
    {
        return new CreateChannelOptions(
            publisherConfirmationsEnabled: false,
            publisherConfirmationTrackingEnabled: false,
            outstandingPublisherConfirmationsRateLimiter: null,
            consumerDispatchConcurrency: ConsumerDispatchConcurrency
        );
    }
}
