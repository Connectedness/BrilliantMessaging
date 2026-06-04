using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Usf.Transport.RabbitMq.Tests.TestSupport;

public sealed class TrackingChannelPool : IRabbitMqChannelPool
{
    private readonly TestRabbitMqChannel? _channel;
    private readonly IList<string> _events;
    private readonly string _name;

    public TrackingChannelPool(string name, IList<string> events, TestRabbitMqChannel? channel = null)
    {
        _name = name ?? throw new ArgumentNullException(nameof(name));
        _events = events ?? throw new ArgumentNullException(nameof(events));
        _channel = channel;
    }

    public int AcquireCallCount { get; private set; }

    public int ReleaseCallCount { get; private set; }

    public ValueTask<RabbitMqChannelLease> AcquireAsync(CancellationToken cancellationToken = default)
    {
        AcquireCallCount++;

        if (_channel is null)
        {
            throw new NotSupportedException();
        }

        return new ValueTask<RabbitMqChannelLease>(new RabbitMqChannelLease(this, _channel.Object));
    }

    public ValueTask ReleaseAsync(in RabbitMqChannelLease lease)
    {
        ReleaseCallCount++;
        return default;
    }

    public ValueTask DisposeAsync()
    {
        _events.Add(_name);
        return default;
    }

    public void Dispose()
    {
        _events.Add(_name);
    }
}
