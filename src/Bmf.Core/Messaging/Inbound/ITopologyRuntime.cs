using System.Threading;
using System.Threading.Tasks;

namespace Bmf.Core.Messaging.Inbound;

/// <summary>
/// A small runtime lifecycle seam for topology instances that have active background work, such as RabbitMQ
/// consumers, NATS subscriptions, SQS polling loops, or Azure Service Bus processors. The topology model itself
/// describes compiled declarations and dispatch definitions; an <see cref="ITopologyRuntime" /> describes the
/// active transport behavior. Publish-only topologies do not register a runtime unless a future transport gains
/// publish-side background work.
/// </summary>
public interface ITopologyRuntime
{
    /// <summary>
    /// Gets the name of the topology this runtime drives.
    /// </summary>
    string TopologyName { get; }

    /// <summary>
    /// Starts the topology's active transport behaviour (for example begins consuming).
    /// </summary>
    /// <param name="cancellationToken">A token to observe while starting.</param>
    /// <returns>A task that completes once the runtime has started.</returns>
    Task StartAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Stops the topology's active transport behaviour, allowing the transport to drain gracefully.
    /// </summary>
    /// <param name="cancellationToken">A token to observe while stopping.</param>
    /// <returns>A task that completes once the runtime has stopped.</returns>
    Task StopAsync(CancellationToken cancellationToken = default);
}
