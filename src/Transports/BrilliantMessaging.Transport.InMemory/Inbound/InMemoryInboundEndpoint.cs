using System;
using BrilliantMessaging.Core.Messaging.Inbound;

namespace BrilliantMessaging.Transport.InMemory.Inbound;

/// <summary>
/// The in-memory base for an inbound endpoint. It specializes the Core <see cref="InboundEndpoint" /> with the
/// fixed <c>in-memory</c> transport name.
/// </summary>
public abstract class InMemoryInboundEndpoint : InboundEndpoint
{
    /// <summary>
    /// The transport name shared by every in-memory outbound target and inbound endpoint.
    /// </summary>
    public const string TransportNameValue = "in-memory";

    /// <summary>
    /// Initializes a new instance of the <see cref="InMemoryInboundEndpoint" /> class.
    /// </summary>
    /// <param name="name">The logical name of the endpoint.</param>
    /// <param name="topologyName">The name of the topology the endpoint belongs to.</param>
    /// <param name="messageType">The message type the endpoint handles.</param>
    /// <param name="handlerType">The handler type.</param>
    /// <param name="deserializerType">The deserializer type.</param>
    /// <param name="discriminator">The CloudEvents discriminator the endpoint is bound to.</param>
    /// <param name="handlerInvocation">The composed pipeline delegate that dispatches the message to the handler.</param>
    /// <param name="ackMode">The acknowledgement mode for the endpoint.</param>
    /// <param name="redeliveryClassifier">The redelivery classifier for handler failures.</param>
    protected InMemoryInboundEndpoint(
        string name,
        string topologyName,
        Type messageType,
        Type handlerType,
        Type deserializerType,
        string discriminator,
        MessageDelegate handlerInvocation,
        MessageAckMode ackMode,
        RedeliveryClassifier redeliveryClassifier
    )
        : base(
            name,
            TransportNameValue,
            topologyName,
            messageType,
            handlerType,
            deserializerType,
            discriminator,
            handlerInvocation,
            ackMode,
            redeliveryClassifier
        ) { }
}

/// <summary>
/// A strongly typed <see cref="InMemoryInboundEndpoint" /> for messages of type <typeparamref name="TMessage" />
/// that validates the handler implements <see cref="IMessageHandler{TMessage}" />.
/// </summary>
/// <typeparam name="TMessage">The message type the endpoint handles.</typeparam>
public sealed class InMemoryInboundEndpoint<TMessage> : InMemoryInboundEndpoint
{
    /// <summary>
    /// Initializes a new instance of the <see cref="InMemoryInboundEndpoint{TMessage}" /> class.
    /// </summary>
    /// <param name="name">The logical name of the endpoint.</param>
    /// <param name="topologyName">The name of the topology the endpoint belongs to.</param>
    /// <param name="handlerType">The handler type, which must implement <see cref="IMessageHandler{TMessage}" />.</param>
    /// <param name="deserializerType">The deserializer type.</param>
    /// <param name="discriminator">The CloudEvents discriminator the endpoint is bound to.</param>
    /// <param name="handlerInvocation">The composed pipeline delegate that dispatches the message to the handler.</param>
    /// <param name="ackMode">The acknowledgement mode for the endpoint.</param>
    /// <param name="redeliveryClassifier">The redelivery classifier for handler failures.</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="handlerType" /> does not implement <see cref="IMessageHandler{TMessage}" />.</exception>
    public InMemoryInboundEndpoint(
        string name,
        string topologyName,
        Type handlerType,
        Type deserializerType,
        string discriminator,
        MessageDelegate handlerInvocation,
        MessageAckMode ackMode,
        RedeliveryClassifier redeliveryClassifier
    )
        : base(
            name,
            topologyName,
            typeof(TMessage),
            handlerType,
            deserializerType,
            discriminator,
            handlerInvocation,
            ackMode,
            redeliveryClassifier
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
