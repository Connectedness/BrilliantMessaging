using System;
using System.Collections.Generic;

namespace Bmf.Core.Messaging;

/// <summary>
/// A registry that exposes exactly one topology under <see cref="Topology.DefaultName" />. It is primarily useful
/// for direct construction in tests and benchmarks where building a full service provider would be overkill.
/// </summary>
public sealed class SingleTopologyRegistry : ITopologyRegistry
{
    private readonly Topology _topology;

    /// <summary>
    /// Initializes a new instance of the <see cref="SingleTopologyRegistry" /> class.
    /// </summary>
    /// <param name="topology">The single topology exposed under <see cref="Topology.DefaultName" />.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="topology" /> is <see langword="null" />.</exception>
    public SingleTopologyRegistry(Topology topology)
    {
        _topology = topology ?? throw new ArgumentNullException(nameof(topology));
        Names = [Topology.DefaultName];
    }

    /// <inheritdoc />
    public IReadOnlyCollection<string> Names { get; }

    /// <inheritdoc />
    public Topology GetRequiredTopology(string name)
    {
        if (TryGetTopology(name, out var topology) && topology is not null)
        {
            return topology;
        }

        throw new InvalidOperationException(
            $"Topology '{name}' is not registered. Registered topologies: {TopologyRegistrationCatalog.FormatNames(Names)}."
        );
    }

    /// <inheritdoc />
    public bool TryGetTopology(string name, out Topology? topology)
    {
        if (string.Equals(name, Topology.DefaultName, StringComparison.Ordinal))
        {
            topology = _topology;
            return true;
        }

        topology = default;
        return false;
    }
}
