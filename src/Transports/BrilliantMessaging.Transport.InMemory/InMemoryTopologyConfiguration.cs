using System;
using System.Collections.Immutable;
using System.Linq;
using BrilliantMessaging.Transport.InMemory.Inbound;
using BrilliantMessaging.Transport.InMemory.Outbound;

namespace BrilliantMessaging.Transport.InMemory;

/// <summary>
/// The immutable, compiled configuration of an in-memory topology, produced by <see cref="InMemoryTopologyBuilder" />.
/// </summary>
/// <param name="Topics">The topics declared with <c>Topic(...)</c>.</param>
/// <param name="Targets">The outbound target declarations.</param>
/// <param name="Consumers">The inbound consumer declarations.</param>
/// <param name="ShutdownTimeout">The graceful shutdown timeout for the topology runtime.</param>
/// <param name="Recording">The routed-message recording behavior for the topology broker.</param>
public sealed record InMemoryTopologyConfiguration(
    ImmutableArray<string> Topics,
    ImmutableArray<InMemoryOutboundTargetDefinition> Targets,
    ImmutableArray<InMemoryInboundConsumerDefinition> Consumers,
    TimeSpan ShutdownTimeout,
    InMemoryRecordingOptions Recording
)
{
    /// <summary>
    /// Gets a value indicating whether the configuration declares any inbound consumers with handlers.
    /// </summary>
    public bool HasInboundEndpoints => Consumers.Any(static consumer => consumer.Handlers.Length > 0);
}
