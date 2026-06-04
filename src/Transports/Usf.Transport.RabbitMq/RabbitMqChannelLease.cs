using System;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace Usf.Transport.RabbitMq;

public readonly struct RabbitMqChannelLease : IAsyncDisposable
{
    private readonly IRabbitMqChannelPool? _pool;

    public RabbitMqChannelLease(IRabbitMqChannelPool pool, IChannel channel, object? state = null, long token = 0)
    {
        _pool = pool ?? throw new ArgumentNullException(nameof(pool));
        Channel = channel ?? throw new ArgumentNullException(nameof(channel));
        State = state;
        Token = token;
    }

    public IChannel Channel =>
        field ?? throw new InvalidOperationException("RabbitMqChannelLease must not be the default instance");

    public object? State { get; }

    public long Token { get; }

    public ValueTask DisposeAsync()
    {
        return _pool?.ReleaseAsync(in this) ?? default;
    }
}
