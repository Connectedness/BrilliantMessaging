using System;
using System.Threading;
using System.Threading.Tasks;

namespace Bmf.Transport.RabbitMq;

/// <summary>
/// A pool of reusable RabbitMQ channels. Callers acquire a <see cref="RabbitMqChannelLease" />, use the channel,
/// and dispose the lease to return the channel to the pool.
/// </summary>
public interface IRabbitMqChannelPool : IAsyncDisposable, IDisposable
{
    /// <summary>
    /// Acquires a channel from the pool, opening a new one if capacity allows or waiting for one to be returned.
    /// </summary>
    /// <param name="cancellationToken">A token to observe while waiting for a channel.</param>
    /// <returns>A lease over the acquired channel.</returns>
    ValueTask<RabbitMqChannelLease> AcquireAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns a leased channel to the pool, or discards it when it is unhealthy or the pool is disposed.
    /// </summary>
    /// <param name="lease">The lease to release.</param>
    /// <returns>A task that completes when the channel has been returned or discarded.</returns>
    ValueTask ReleaseAsync(in RabbitMqChannelLease lease);
}
