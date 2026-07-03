using System;
using System.Collections.Immutable;
using BrilliantMessaging.Core.Messaging.Inbound;

namespace BrilliantMessaging.Transport.Nats.Inbound;

/// <summary>
/// A durable JetStream consumer definition.
/// </summary>
public sealed record NatsInboundConsumerDefinition(
    string StreamName,
    string DurableName,
    string? FilterSubject,
    int Concurrency,
    TimeSpan AckWait,
    int MaxDeliver,
    int MaxAckPending,
    string? DeadLetterSubject,
    RedeliveryClassifier? RedeliveryClassifier,
    ImmutableArray<NatsInboundHandlerDefinition> Handlers
);

/// <summary>
/// A typed handler bound to a NATS durable consumer.
/// </summary>
public sealed record NatsInboundHandlerDefinition(
    string? EndpointName,
    Type MessageType,
    Type HandlerType,
    MessageDelegate HandlerInvocation,
    Type DeserializerType,
    MessageAckMode AckMode,
    RedeliveryClassifier? RedeliveryClassifier
);

/// <summary>
/// Handler-level NATS inbound configuration.
/// </summary>
public sealed record NatsInboundHandlerConfiguration(
    Type DeserializerType,
    MessageAckMode AckMode,
    RedeliveryClassifier? RedeliveryClassifier
);
