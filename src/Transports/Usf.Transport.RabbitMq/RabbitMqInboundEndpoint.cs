using System;
using Usf.Core.Messaging;

namespace Usf.Transport.RabbitMq;

public abstract class RabbitMqInboundEndpoint : InboundEndpoint
{
    protected RabbitMqInboundEndpoint(
        string name,
        string topologyName,
        Type messageType,
        Type handlerType,
        Type deserializerType,
        string discriminator,
        MessageDelegate handlerInvocation,
        MessageAckMode ackMode
    )
        : base(
            name,
            "rabbitmq",
            topologyName,
            messageType,
            handlerType,
            deserializerType,
            discriminator,
            handlerInvocation,
            ackMode
        ) { }
}

public sealed class RabbitMqInboundEndpoint<TMessage> : RabbitMqInboundEndpoint
{
    public RabbitMqInboundEndpoint(
        string name,
        string topologyName,
        Type handlerType,
        Type deserializerType,
        string discriminator,
        MessageDelegate handlerInvocation,
        MessageAckMode ackMode
    )
        : base(
            name,
            topologyName,
            typeof(TMessage),
            handlerType,
            deserializerType,
            discriminator,
            handlerInvocation,
            ackMode
        )
    {
        if (!typeof(IMessageHandler<TMessage>).IsAssignableFrom(handlerType))
        {
            throw new ArgumentException(
                $"Handler type '{handlerType}' must implement '{typeof(IMessageHandler<TMessage>)}'.",
                nameof(handlerType)
            );
        }
    }
}
