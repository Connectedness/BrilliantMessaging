using System.Collections.Generic;

namespace Bmf.Core.Messaging;

/// <summary>
/// Resolves registered topology instances by name. Both publishing topologies and consuming-only topologies are
/// reachable here for observability, tests, validation, and management APIs; the active behavior of consuming
/// topologies is started through <see cref="Inbound.ITopologyRuntime" /> services rather than normal publish call sites.
/// </summary>
public interface ITopologyRegistry
{
    /// <summary>
    /// Gets the names of the registered topologies.
    /// </summary>
    IReadOnlyCollection<string> Names { get; }

    /// <summary>
    /// Gets the topology registered under the given name.
    /// </summary>
    /// <param name="name">The topology name.</param>
    /// <returns>The matching topology.</returns>
    /// <exception cref="System.InvalidOperationException">Thrown when no topology is registered under <paramref name="name" />.</exception>
    Topology GetRequiredTopology(string name);

    /// <summary>
    /// Attempts to get the topology registered under the given name.
    /// </summary>
    /// <param name="name">The topology name.</param>
    /// <param name="topology">When this method returns, the matching topology, or <see langword="null" /> when none was found.</param>
    /// <returns><see langword="true" /> when a topology was found; otherwise <see langword="false" />.</returns>
    bool TryGetTopology(string name, out Topology? topology);
}
