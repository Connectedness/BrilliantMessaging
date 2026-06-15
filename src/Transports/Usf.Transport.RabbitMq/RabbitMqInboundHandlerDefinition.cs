using System;
using Usf.Core.Messaging;

namespace Usf.Transport.RabbitMq;

public sealed record RabbitMqInboundHandlerDefinition(
    string? EndpointName,
    Type MessageType,
    Type HandlerType,
    MessageDelegate HandlerInvocation,
    Type DeserializerType,
    MessageAckMode AckMode
);
