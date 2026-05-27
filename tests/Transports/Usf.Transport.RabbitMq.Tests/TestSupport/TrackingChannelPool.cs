using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Usf.Transport.RabbitMq.Tests.TestSupport;

public sealed class TrackingChannelPool : IRabbitMqChannelPool
{
    private readonly IList<string> _events;
    private readonly string _name;

    public TrackingChannelPool(string name, IList<string> events)
    {
        _name = name ?? throw new ArgumentNullException(nameof(name));
        _events = events ?? throw new ArgumentNullException(nameof(events));
    }

    public ValueTask<RabbitMqChannelLease> AcquireAsync(CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException();
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
