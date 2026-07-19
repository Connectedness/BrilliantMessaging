using System;
using System.Collections.Generic;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;
using BrilliantMessaging.Transport.Nats.Inbound;
using BrilliantMessaging.Transport.Nats.Outbound;
using NATS.Client.Core;

namespace BrilliantMessaging.Transport.Nats;

/// <summary>
/// The compiled configuration for a NATS topology. This transport implements JetStream-backed messaging only;
/// core NATS pub/sub is intentionally not configured by this package.
/// </summary>
public sealed record NatsTopologyConfiguration(
    Func<IServiceProvider, NatsOpts>? CreateOptions,
    IReadOnlyList<NatsStreamDefinition> Streams,
    IReadOnlyList<NatsOutboundTargetDefinition> Targets,
    IReadOnlyList<NatsInboundConsumerDefinition> Consumers,
    Type DeserializationMiddlewareType,
    Action<MessagePipelineBuilder>? ConfigurePipeline,
    TimeSpan ShutdownTimeout,
    NatsTopologyProvisioningMode ProvisioningMode,
    bool AckProgressEnabled,
    MessageContractRegistry? MessageContractDialect = null
)
{
    /// <summary>
    /// Gets a value indicating whether this topology has any inbound consumers.
    /// </summary>
    public bool HasInboundEndpoints => Consumers.Count > 0;
}

/// <summary>
/// Controls whether Brilliant Messaging creates/updates JetStream topology or only asserts that it exists.
/// </summary>
public enum NatsTopologyProvisioningMode
{
    /// <summary>
    /// Create or update streams and durable consumers idempotently where JetStream permits it.
    /// </summary>
    CreateOrUpdate = 0,

    /// <summary>
    /// Assert that externally managed streams and durable consumers already exist.
    /// </summary>
    AssertOnly = 1
}

/// <summary>
/// Storage policy for declared JetStream streams.
/// </summary>
public enum NatsStreamStorage
{
    /// <summary>
    /// Persist stream data to the server's configured file store.
    /// </summary>
    File = 0,

    /// <summary>
    /// Keep stream data in memory.
    /// </summary>
    Memory = 1
}

/// <summary>
/// Retention policy for declared JetStream streams.
/// </summary>
public enum NatsStreamRetention
{
    /// <summary>
    /// Retain messages according to stream limits.
    /// </summary>
    Limits = 0,

    /// <summary>
    /// Retain messages while consumers still have interest.
    /// </summary>
    Interest = 1,

    /// <summary>
    /// Retain messages until one work-queue consumer acknowledges them.
    /// </summary>
    WorkQueue = 2
}

/// <summary>
/// A declared JetStream stream.
/// </summary>
public sealed record NatsStreamDefinition(
    string Name,
    IReadOnlyList<string> Subjects,
    TimeSpan? DuplicateWindow,
    TimeSpan? MaxAge,
    int? MaxMessageSize,
    NatsStreamStorage Storage,
    NatsStreamRetention Retention,
    int Replicas
);

/// <summary>
/// Shared NATS option helpers used by all NATS topology builders.
/// </summary>
public static class NatsTopologyBuilderDefaults
{
    /// <summary>
    /// The minimum replica count accepted by JetStream streams.
    /// </summary>
    public const int MinimumStreamReplicas = 1;

    /// <summary>
    /// The maximum replica count accepted by JetStream streams.
    /// </summary>
    public const int MaximumStreamReplicas = 5;

    /// <summary>
    /// The default NATS server URI used when no explicit endpoint is configured.
    /// </summary>
    public const string DefaultServerUrl = "nats://localhost:4222";

    /// <summary>
    /// The default number of messages a consumer worker buffers client-side per pull request. Buffered
    /// messages are not heartbeated while they wait for the sequential dispatch loop, so their AckWait
    /// keeps running from server delivery; the default is deliberately small to keep the last buffered
    /// message well within AckWait for typical handler durations.
    /// </summary>
    public const int DefaultMaxBufferedMessages = 8;

    /// <summary>
    /// The default graceful shutdown timeout for inbound topology runtimes.
    /// </summary>
    public static readonly TimeSpan DefaultShutdownTimeout = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The default JetStream AckWait for durable consumers.
    /// </summary>
    public static readonly TimeSpan DefaultAckWait = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The smallest accepted JetStream AckWait. The AckProgress heartbeat runs at <c>AckWait / 3</c> with
    /// a floor of one second; below three seconds the floor takes over and the margin shrinks until the
    /// first heartbeat races or misses the ack deadline entirely, so shorter values are rejected.
    /// </summary>
    public static readonly TimeSpan MinimumAckWait = TimeSpan.FromSeconds(3);
}
