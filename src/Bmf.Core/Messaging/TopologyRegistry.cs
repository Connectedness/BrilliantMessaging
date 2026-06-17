using System;
using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;

namespace Bmf.Core.Messaging;

/// <summary>
/// The default <see cref="ITopologyRegistry" />. It tracks registered topology names through a
/// <see cref="TopologyRegistrationCatalog" /> and resolves each topology instance as a keyed service from the
/// container on demand.
/// </summary>
public sealed class TopologyRegistry : ITopologyRegistry
{
    private readonly TopologyRegistrationCatalog _catalog;
    private readonly IServiceProvider _serviceProvider;

    /// <summary>
    /// Initializes a new instance of the <see cref="TopologyRegistry" /> class.
    /// </summary>
    /// <param name="serviceProvider">The container used to resolve keyed topology instances.</param>
    /// <param name="catalog">The catalog of registered topology names.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="serviceProvider" /> or <paramref name="catalog" /> is <see langword="null" />.</exception>
    public TopologyRegistry(IServiceProvider serviceProvider, TopologyRegistrationCatalog catalog)
    {
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        _catalog = catalog ?? throw new ArgumentNullException(nameof(catalog));
    }

    /// <inheritdoc />
    public IReadOnlyCollection<string> Names => _catalog.Names;

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
        if (!_catalog.Contains(name))
        {
            topology = default;
            return false;
        }

        topology = _serviceProvider.GetRequiredKeyedService<Topology>(name);
        return true;
    }
}
