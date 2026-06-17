using System;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace Bmf.Transport.RabbitMq.Outbound;

/// <summary>
/// A pool of publish channels that share a publisher-confirm mode and timeout. Outbound targets acquire a
/// channel from their group for each publish, so the group's maximum channel count bounds publish concurrency.
/// </summary>
public sealed class RabbitMqOutboundChannelGroup : IAsyncDisposable, IDisposable
{
    private readonly IRabbitMqChannelPool _channelPool;

    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMqOutboundChannelGroup" /> class.
    /// </summary>
    /// <param name="name">The group name.</param>
    /// <param name="maximumChannelCount">The maximum number of channels the group may open; must be greater than zero.</param>
    /// <param name="channelFactory">A factory that opens a new channel for the group.</param>
    /// <param name="publisherConfirmMode">The publisher-confirm mode for channels in the group.</param>
    /// <param name="publisherConfirmTimeout">The bounded wait for publisher confirmations, or <see langword="null" /> for the default.</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="name" /> is null or whitespace.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="maximumChannelCount" /> is less than one, <paramref name="publisherConfirmMode" /> is undefined, or the confirm timeout is out of range.</exception>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="channelFactory" /> is <see langword="null" />.</exception>
    public RabbitMqOutboundChannelGroup(
        string name,
        int maximumChannelCount,
        Func<CancellationToken, Task<IChannel>> channelFactory,
        RabbitMqPublisherConfirmMode publisherConfirmMode = RabbitMqPublisherConfirmDefaults.Mode,
        TimeSpan? publisherConfirmTimeout = null
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

        if (!Enum.IsDefined(typeof(RabbitMqPublisherConfirmMode), publisherConfirmMode))
        {
            throw new ArgumentOutOfRangeException(
                nameof(publisherConfirmMode),
                publisherConfirmMode,
                "Unsupported publisher confirm mode."
            );
        }

        var resolvedPublisherConfirmTimeout = publisherConfirmTimeout ?? RabbitMqPublisherConfirmDefaults.Timeout;

        if (!RabbitMqPublisherConfirmDefaults.IsValidTimeout(resolvedPublisherConfirmTimeout))
        {
            throw new ArgumentOutOfRangeException(
                nameof(publisherConfirmTimeout),
                resolvedPublisherConfirmTimeout,
                "The value must be finite and greater than zero."
            );
        }

        Name = name;
        MaximumChannelCount = maximumChannelCount;
        PublisherConfirmMode = publisherConfirmMode;
        PublisherConfirmTimeout = resolvedPublisherConfirmTimeout;
        _channelPool = new DefaultRabbitMqChannelPool(
            maximumChannelCount,
            channelFactory ?? throw new ArgumentNullException(nameof(channelFactory))
        );
    }

    /// <summary>
    /// Gets the group name.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Gets the maximum number of channels the group may open.
    /// </summary>
    public int MaximumChannelCount { get; }

    /// <summary>
    /// Gets the publisher-confirm mode for channels in the group.
    /// </summary>
    public RabbitMqPublisherConfirmMode PublisherConfirmMode { get; }

    /// <summary>
    /// Gets the bounded wait applied to publisher confirmations.
    /// </summary>
    public TimeSpan PublisherConfirmTimeout { get; }

    /// <summary>
    /// Asynchronously disposes the group and its channel pool.
    /// </summary>
    /// <returns>A task that completes once the pool is disposed.</returns>
    public ValueTask DisposeAsync()
    {
        return _channelPool.DisposeAsync();
    }

    /// <summary>
    /// Disposes the group and its channel pool.
    /// </summary>
    public void Dispose()
    {
        _channelPool.Dispose();
    }

    /// <summary>
    /// Acquires a publish channel from the group's pool.
    /// </summary>
    /// <param name="cancellationToken">A token to observe while waiting for a channel.</param>
    /// <returns>A lease over the acquired channel.</returns>
    public ValueTask<RabbitMqChannelLease> AcquireAsync(CancellationToken cancellationToken = default)
    {
        return _channelPool.AcquireAsync(cancellationToken);
    }
}
