using System;
using BrilliantMessaging.Core.Messaging.Inbound;

namespace BrilliantMessaging.Transport.RabbitMq.Inbound;

/// <summary>
/// The compiled handler-level configuration produced by <see cref="RabbitMqInboundHandlerBuilder" /> before the
/// consumer attaches the message type, handler type, and endpoint name.
/// </summary>
/// <param name="DeserializerType">The deserializer type for the handler.</param>
/// <param name="AckMode">The acknowledgement mode for the handler.</param>
/// <param name="RedeliveryClassifier">
/// The explicit redelivery classifier for the handler, or <see langword="null" /> to inherit from the consumer or
/// queue type.
/// </param>
public readonly record struct RabbitMqInboundHandlerConfiguration(
    Type DeserializerType,
    MessageAckMode AckMode,
    RedeliveryClassifier? RedeliveryClassifier
);
