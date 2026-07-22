using System;

namespace BrilliantMessaging.Transport.Nats.Outbound;

/// <summary>
/// Immutable NATS outbound target definition.
/// </summary>
public sealed record NatsOutboundTargetDefinition(
    Type MessageType,
    string Subject,
    string? TargetName,
    Type? SerializerType,
    bool MessageIdDeduplication
);
