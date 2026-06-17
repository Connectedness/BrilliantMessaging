using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

namespace Bmf.Core.Messaging;

/// <summary>
/// Tracks the registered topology names. Topology names form a single ordinal string namespace that matches the
/// one-connection/client ownership boundary, so registering the same name twice fails even when
/// one registration is publish-only and the other is consume-only.
/// </summary>
public sealed class TopologyRegistrationCatalog
{
    private readonly List<string> _names = [];
    private readonly HashSet<string> _namesSet = new (StringComparer.Ordinal);

    /// <summary>
    /// Gets the registered topology names in registration order.
    /// </summary>
    public IReadOnlyCollection<string> Names => new ReadOnlyCollection<string>(_names);

    /// <summary>
    /// Registers a topology name.
    /// </summary>
    /// <param name="name">The topology name to register.</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="name" /> is null or whitespace.</exception>
    /// <exception cref="InvalidOperationException">Thrown when <paramref name="name" /> is already registered.</exception>
    public void Add(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(name));
        }

        if (!_namesSet.Add(name))
        {
            throw new InvalidOperationException(
                $"Topology '{name}' is already registered. Registered topologies: {FormatNames(_names)}."
            );
        }

        _names.Add(name);
    }

    /// <summary>
    /// Determines whether the given topology name is registered.
    /// </summary>
    /// <param name="name">The topology name to check.</param>
    /// <returns><see langword="true" /> when the name is registered; otherwise <see langword="false" />.</returns>
    public bool Contains(string name)
    {
        return _namesSet.Contains(name);
    }

    /// <summary>
    /// Formats a set of topology names for display in diagnostics, ordered and comma-separated, or <c>(none)</c>
    /// when empty.
    /// </summary>
    /// <param name="names">The names to format.</param>
    /// <returns>The formatted name list.</returns>
    public static string FormatNames(IEnumerable<string> names)
    {
        var values = names
           .OrderBy(static value => value, StringComparer.Ordinal)
           .ToArray();

        return values.Length == 0 ? "(none)" : string.Join(", ", values);
    }
}
