using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using Bmf.Core.Messaging.Inbound;
using Bmf.Core.Messaging.Outbound;

namespace Bmf.Core.Messaging;

/// <summary>
/// The compiled, lookup-ready data backing a <see cref="Topology" />: outbound targets indexed by message type
/// and by name, inbound endpoints indexed by name, and the flattened target/endpoint arrays.
/// </summary>
/// <param name="TargetsByMessageType">Outbound targets indexed by the message type they publish.</param>
/// <param name="TargetsByName">Outbound targets indexed by their name.</param>
/// <param name="EndpointsByName">Inbound endpoints indexed by their name.</param>
/// <param name="OutboundTargets">All distinct outbound targets.</param>
/// <param name="InboundEndpoints">All inbound endpoints.</param>
public readonly record struct TopologyData(
    FrozenDictionary<Type, OutboundTarget> TargetsByMessageType,
    FrozenDictionary<string, OutboundTarget> TargetsByName,
    FrozenDictionary<string, InboundEndpoint> EndpointsByName,
    ImmutableArray<OutboundTarget> OutboundTargets,
    ImmutableArray<InboundEndpoint> InboundEndpoints
)
{
    /// <summary>
    /// Builds the frozen lookup structures from the supplied mutable dictionaries, producing a ready-to-use
    /// <see cref="TopologyData" />.
    /// </summary>
    /// <param name="targetsByMessageType">Outbound targets keyed by message type.</param>
    /// <param name="targetsByName">Outbound targets keyed by name.</param>
    /// <param name="endpointsByName">Inbound endpoints keyed by name.</param>
    /// <returns>The compiled topology data.</returns>
    /// <exception cref="ArgumentNullException">Thrown when any argument is <see langword="null" />.</exception>
    public static TopologyData PrepareTopologyDataStructures(
        IDictionary<Type, OutboundTarget> targetsByMessageType,
        IDictionary<string, OutboundTarget> targetsByName,
        IDictionary<string, InboundEndpoint> endpointsByName
    )
    {
        if (targetsByMessageType is null)
        {
            throw new ArgumentNullException(nameof(targetsByMessageType));
        }

        if (targetsByName is null)
        {
            throw new ArgumentNullException(nameof(targetsByName));
        }

        if (endpointsByName is null)
        {
            throw new ArgumentNullException(nameof(endpointsByName));
        }

        var frozenTargetsByMessageType = targetsByMessageType.ToFrozenDictionary();
        var frozenTargetsByName = targetsByName.ToFrozenDictionary(StringComparer.Ordinal);
        var frozenEndpointsByName = endpointsByName.ToFrozenDictionary(StringComparer.Ordinal);

        return new TopologyData(
            frozenTargetsByMessageType,
            frozenTargetsByName,
            frozenEndpointsByName,
            [..frozenTargetsByMessageType.Values.Concat(frozenTargetsByName.Values).Distinct()],
            [..frozenEndpointsByName.Values]
        );
    }
}
