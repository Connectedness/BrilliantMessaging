using System;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace Usf.Transport.RabbitMq;

public readonly struct RabbitMqChannelLease : IAsyncDisposable
{
    private readonly DefaultRabbitMqChannelPool _pool;
    private readonly long _leaseId;
    private readonly DefaultRabbitMqChannelPool.PooledChannel _pooledChannel;

    public RabbitMqChannelLease(DefaultRabbitMqChannelPool pool, DefaultRabbitMqChannelPool.PooledChannel pooledChannel, long leaseId)
    {
        _pool = pool ?? throw new ArgumentNullException(nameof(pool));
        _pooledChannel = pooledChannel ?? throw new ArgumentNullException(nameof(pooledChannel));
        _leaseId = leaseId;
    }

    public IChannel Channel =>
        _pooledChannel.Channel ?? throw new ObjectDisposedException(nameof(RabbitMqChannelLease));

    public ValueTask DisposeAsync()
    {
        if (_pool is null || _pooledChannel is null)
        {
            return default;
        }

        return _pool.ReleaseAsync(_pooledChannel, _leaseId);
    }
}
