using System;
using Bmf.Core.Messaging.Inbound;

namespace Bmf.Transport.RabbitMq.Inbound;

/// <summary>
/// An immutable declaration of a single handler within a consumer, produced by <see cref="RabbitMqInboundConsumerBuilder" />.
/// </summary>
/// <param name="EndpointName">The explicit endpoint name, or <see langword="null" /> to derive one.</param>
/// <param name="MessageType">The message type the handler processes.</param>
/// <param name="HandlerType">The concrete handler type.</param>
/// <param name="HandlerInvocation">The pipeline delegate that dispatches a message to the handler.</param>
/// <param name="DeserializerType">The deserializer type for the handler.</param>
/// <param name="AckMode">The acknowledgement mode for the handler.</param>
public sealed record RabbitMqInboundHandlerDefinition(
    string? EndpointName,
    Type MessageType,
    Type HandlerType,
    MessageDelegate HandlerInvocation,
    Type DeserializerType,
    MessageAckMode AckMode
);
