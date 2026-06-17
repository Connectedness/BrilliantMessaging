using System;
using System.Collections.Generic;

namespace Bmf.Core.Messaging.Inbound;

/// <summary>
/// Compares <see cref="InboundEndpointSelectionKey" /> values using ordinal string comparison on both the
/// source and the discriminator.
/// </summary>
public sealed class InboundEndpointSelectionKeyComparer : IEqualityComparer<InboundEndpointSelectionKey>
{
    /// <summary>
    /// Gets the shared comparer instance.
    /// </summary>
    public static InboundEndpointSelectionKeyComparer Instance { get; } = new ();

    /// <inheritdoc />
    public bool Equals(InboundEndpointSelectionKey x, InboundEndpointSelectionKey y)
    {
        return string.Equals(x.Source, y.Source, StringComparison.Ordinal) &&
               string.Equals(x.Discriminator, y.Discriminator, StringComparison.Ordinal);
    }

    /// <inheritdoc />
    public int GetHashCode(InboundEndpointSelectionKey obj) => HashCode.Combine(obj.Source, obj.Discriminator);
}
