using System;
using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging.Inbound;

namespace BrilliantMessaging.Transport.InMemory;

/// <summary>
/// The active runtime for an in-memory topology that contains inbound consumers. It starts and stops the broker's
/// background workers through the existing <see cref="ITopologyRuntime" /> start/stop model and is driven by the
/// shared <see cref="TopologyRuntimeHostedService" />. On stop it drains in-flight deliveries against the
/// topology's configured shutdown timeout before cancelling the remaining work.
/// </summary>
public sealed class InMemoryTopologyRuntime : ITopologyRuntime
{
    private readonly InMemoryTopology _topology;

    /// <summary>
    /// Initializes a new instance of the <see cref="InMemoryTopologyRuntime" /> class.
    /// </summary>
    /// <param name="topology">The topology whose broker workers this runtime drives.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="topology" /> is <see langword="null" />.</exception>
    public InMemoryTopologyRuntime(InMemoryTopology topology)
    {
        _topology = topology ?? throw new ArgumentNullException(nameof(topology));
    }

    /// <inheritdoc />
    public string TopologyName => _topology.Name;

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        return _topology.Broker.StartAsync(cancellationToken);
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken = default)
    {
        return _topology.Broker.StopAsync(cancellationToken);
    }
}
