using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Usf.Core.Messaging;
using Usf.Transport.RabbitMq.Configuration;

namespace Usf.Transport.RabbitMq;

public sealed class RabbitMqCompiledTopology : IAsyncDisposable, IDisposable
{
    public RabbitMqCompiledTopology(
        MessageTopology messageTopology,
        IReadOnlyList<RabbitMqExchangeDefinition> exchanges,
        IReadOnlyList<RabbitMqQueueDefinition> queues,
        IReadOnlyList<RabbitMqBindingDefinition> bindings,
        IReadOnlyList<Target> targets,
        IRabbitMqChannelPool? sharedChannelPool
    )
    {
        MessageTopology = messageTopology ?? throw new ArgumentNullException(nameof(messageTopology));
        Exchanges = exchanges ?? throw new ArgumentNullException(nameof(exchanges));
        Queues = queues ?? throw new ArgumentNullException(nameof(queues));
        Bindings = bindings ?? throw new ArgumentNullException(nameof(bindings));
        _targets = targets ?? throw new ArgumentNullException(nameof(targets));
        _sharedChannelPool = sharedChannelPool;
    }

    private readonly IRabbitMqChannelPool? _sharedChannelPool;
    private readonly IReadOnlyList<Target> _targets;

    public MessageTopology MessageTopology { get; }

    public IReadOnlyList<RabbitMqExchangeDefinition> Exchanges { get; }

    public IReadOnlyList<RabbitMqQueueDefinition> Queues { get; }

    public IReadOnlyList<RabbitMqBindingDefinition> Bindings { get; }

    public async ValueTask DisposeAsync()
    {
        foreach (var target in _targets)
        {
            if (target is IAsyncDisposable asyncDisposable)
            {
                await asyncDisposable.DisposeAsync().ConfigureAwait(false);
            }
            else if (target is IDisposable disposable)
            {
                disposable.Dispose();
            }
        }

        if (_sharedChannelPool is not null)
        {
            await _sharedChannelPool.DisposeAsync().ConfigureAwait(false);
        }
    }

    public void Dispose()
    {
        foreach (var target in _targets)
        {
            if (target is IDisposable disposable)
            {
                disposable.Dispose();
            }
        }

        _sharedChannelPool?.Dispose();
    }
}
