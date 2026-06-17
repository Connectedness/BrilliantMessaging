using System;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace Bmf.Transport.RabbitMq;

/// <summary>
/// A lease over a pooled RabbitMQ channel. Use <see cref="Channel" /> while the lease is held and dispose the
/// lease (ideally with <c>await using</c>) to return the channel to its pool.
/// </summary>
public readonly struct RabbitMqChannelLease : IAsyncDisposable
{
    private readonly IRabbitMqChannelPool? _pool;

    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMqChannelLease" /> struct.
    /// </summary>
    /// <param name="pool">The pool the channel is returned to on disposal.</param>
    /// <param name="channel">The leased channel.</param>
    /// <param name="state">Pool-owned state associated with the lease, or <see langword="null" />.</param>
    /// <param name="token">A token identifying this lease, used by the pool to guard against double-release.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="pool" /> or <paramref name="channel" /> is <see langword="null" />.</exception>
    public RabbitMqChannelLease(IRabbitMqChannelPool pool, IChannel channel, object? state = null, long token = 0)
    {
        _pool = pool ?? throw new ArgumentNullException(nameof(pool));
        Channel = channel ?? throw new ArgumentNullException(nameof(channel));
        State = state;
        Token = token;
    }

    /// <summary>
    /// Gets the leased channel.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when accessed on a default-constructed lease.</exception>
    public IChannel Channel =>
        field ?? throw new InvalidOperationException("RabbitMqChannelLease must not be the default instance");

    /// <summary>
    /// Gets the pool-owned state associated with the lease.
    /// </summary>
    public object? State { get; }

    /// <summary>
    /// Gets the token identifying this lease.
    /// </summary>
    public long Token { get; }

    /// <summary>
    /// Returns the channel to its pool.
    /// </summary>
    /// <returns>A task that completes once the channel has been returned.</returns>
    public ValueTask DisposeAsync()
    {
        return _pool?.ReleaseAsync(in this) ?? default;
    }
}
