using System;
using System.Threading;
using System.Threading.Tasks;

namespace Usf.Transport.RabbitMq;

public interface IRabbitMqChannelPool : IAsyncDisposable, IDisposable
{
    ValueTask<RabbitMqChannelLease> AcquireAsync(CancellationToken cancellationToken = default);

    ValueTask ReleaseAsync(in RabbitMqChannelLease lease);
}
