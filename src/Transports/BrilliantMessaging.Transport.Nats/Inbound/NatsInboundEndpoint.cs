using System;
using BrilliantMessaging.Core.Messaging.Inbound;

namespace BrilliantMessaging.Transport.Nats.Inbound;

/// <summary>
/// A NATS inbound endpoint.
/// </summary>
public sealed class NatsInboundEndpoint<TMessage> : NatsInboundEndpoint
{
    /// <summary>
    /// Initializes a new instance of the <see cref="NatsInboundEndpoint{TMessage}" /> class.
    /// </summary>
    public NatsInboundEndpoint(
        string name,
        string topologyName,
        string subject,
        Type handlerType,
        Type deserializerType,
        string discriminator,
        MessageDelegate handlerInvocation,
        MessageAckMode ackMode,
        RedeliveryClassifier redeliveryClassifier
    ) : base(
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

        Subject = subject;
    }

    /// <summary>
    /// Gets the NATS source subject this endpoint handles.
    /// </summary>
    public override string Subject { get; }
}

/// <summary>
/// Non-generic NATS inbound endpoint base.
/// </summary>
public abstract class NatsInboundEndpoint : InboundEndpoint
{
    private protected NatsInboundEndpoint(
        string name,
        string topologyName,
        Type messageType,
        Type handlerType,
        Type deserializerType,
        string discriminator,
        MessageDelegate handlerInvocation,
        MessageAckMode ackMode,
        RedeliveryClassifier redeliveryClassifier
    ) : base(
        name,
        NatsTopology.TransportNameValue,
        topologyName,
        messageType,
        handlerType,
        deserializerType,
        discriminator,
        handlerInvocation,
        ackMode,
        redeliveryClassifier
    ) { }

    /// <summary>
    /// Gets the NATS source subject this endpoint handles.
    /// </summary>
    public abstract string Subject { get; }
}
