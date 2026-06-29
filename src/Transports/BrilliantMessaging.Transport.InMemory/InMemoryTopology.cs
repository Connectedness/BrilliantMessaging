using System;
using System.Collections.Generic;
using BrilliantMessaging.Core.Messaging;

namespace BrilliantMessaging.Transport.InMemory;

/// <summary>
/// The compiled in-memory topology. It extends the Core <see cref="Topology" /> with the in-memory runtime state:
/// the consumer routes, the graceful shutdown timeout, and the <see cref="InMemoryBroker" /> that records routed
/// messages and dispatches deliveries. This constructor is invoked by the topology compiler with fully resolved
/// state and is not intended to be called directly.
/// </summary>
public sealed class InMemoryTopology : Topology
{
    /// <summary>
    /// Initializes a new instance of the <see cref="InMemoryTopology" /> class.
    /// </summary>
    /// <param name="name">The topology name.</param>
    /// <param name="data">The compiled Core topology data (targets and endpoints).</param>
    /// <param name="broker">The broker that records and dispatches routed messages.</param>
    /// <param name="routes">The compiled consumer routes.</param>
    /// <param name="shutdownTimeout">The graceful shutdown timeout for the topology runtime.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="broker" /> or <paramref name="routes" /> is <see langword="null" />.</exception>
    public InMemoryTopology(
        string name,
        TopologyData data,
        InMemoryBroker broker,
        IReadOnlyList<InMemoryConsumerRoute> routes,
        TimeSpan shutdownTimeout
    )
        : base(name, data)
    {
        Broker = broker ?? throw new ArgumentNullException(nameof(broker));
        Routes = routes ?? throw new ArgumentNullException(nameof(routes));
        ShutdownTimeout = shutdownTimeout;
    }

    /// <summary>
    /// Gets the broker that records routed messages and dispatches deliveries for this topology.
    /// </summary>
    public InMemoryBroker Broker { get; }

    /// <summary>
    /// Gets the compiled consumer routes.
    /// </summary>
    public IReadOnlyList<InMemoryConsumerRoute> Routes { get; }

    /// <summary>
    /// Gets the graceful shutdown timeout for the topology runtime.
    /// </summary>
    public TimeSpan ShutdownTimeout { get; }
}
