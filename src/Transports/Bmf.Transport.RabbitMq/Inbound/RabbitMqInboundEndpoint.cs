using System;
using Bmf.Core.Messaging.Inbound;

namespace Bmf.Transport.RabbitMq.Inbound;

/// <summary>
/// The RabbitMQ base for an inbound endpoint. It specializes the Core <see cref="InboundEndpoint" /> with the
/// fixed <c>rabbitmq</c> transport name; transport authors extending the inbound model derive from it.
/// </summary>
public abstract class RabbitMqInboundEndpoint : InboundEndpoint
{
    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMqInboundEndpoint" /> class.
    /// </summary>
    /// <param name="name">The logical name of the endpoint.</param>
    /// <param name="topologyName">The name of the topology the endpoint belongs to.</param>
    /// <param name="messageType">The message type the endpoint handles.</param>
    /// <param name="handlerType">The handler type.</param>
    /// <param name="deserializerType">The deserializer type.</param>
    /// <param name="discriminator">The CloudEvents discriminator the endpoint is bound to.</param>
    /// <param name="handlerInvocation">The composed pipeline delegate that dispatches the message to the handler.</param>
    /// <param name="ackMode">The acknowledgement mode for the endpoint.</param>
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

/// <summary>
/// A strongly typed <see cref="RabbitMqInboundEndpoint" /> for messages of type <typeparamref name="TMessage" />
/// that validates the handler implements <see cref="IMessageHandler{TMessage}" />.
/// </summary>
/// <typeparam name="TMessage">The message type the endpoint handles.</typeparam>
public sealed class RabbitMqInboundEndpoint<TMessage> : RabbitMqInboundEndpoint
{
    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMqInboundEndpoint{TMessage}" /> class.
    /// </summary>
    /// <param name="name">The logical name of the endpoint.</param>
    /// <param name="topologyName">The name of the topology the endpoint belongs to.</param>
    /// <param name="handlerType">The handler type, which must implement <see cref="IMessageHandler{TMessage}" />.</param>
    /// <param name="deserializerType">The deserializer type.</param>
    /// <param name="discriminator">The CloudEvents discriminator the endpoint is bound to.</param>
    /// <param name="handlerInvocation">The composed pipeline delegate that dispatches the message to the handler.</param>
    /// <param name="ackMode">The acknowledgement mode for the endpoint.</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="handlerType" /> does not implement <see cref="IMessageHandler{TMessage}" />.</exception>
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
