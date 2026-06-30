using System;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;

namespace BrilliantMessaging.Transport.Nats.Inbound;

/// <summary>
/// Configures a NATS inbound handler.
/// </summary>
public sealed class NatsInboundHandlerBuilder : IBuildable<NatsInboundHandlerConfiguration>
{
    private MessageAckMode _ackMode = MessageAckMode.Auto;
    private Type _deserializerType = typeof(PayloadCodecMessageDeserializer);
    private RedeliveryClassifier? _redeliveryClassifier;

    /// <inheritdoc />
    NatsInboundHandlerConfiguration IBuildable<NatsInboundHandlerConfiguration>.Build()
    {
        return new NatsInboundHandlerConfiguration(_deserializerType, _ackMode, _redeliveryClassifier);
    }

    /// <summary>
    /// Overrides the deserializer used by this handler.
    /// </summary>
    public NatsInboundHandlerBuilder WithDeserializer<TDeserializer>()
        where TDeserializer : class, IMessageDeserializer
    {
        _deserializerType = typeof(TDeserializer);
        return this;
    }

    /// <summary>
    /// Lets the handler settle the JetStream message manually through <see cref="IMessageAcknowledgement" />.
    /// </summary>
    public NatsInboundHandlerBuilder ManualAck()
    {
        _ackMode = MessageAckMode.Manual;
        return this;
    }

    /// <summary>
    /// Restores automatic acknowledgement after successful handler completion.
    /// </summary>
    public NatsInboundHandlerBuilder AutoAck()
    {
        _ackMode = MessageAckMode.Auto;
        return this;
    }

    /// <summary>
    /// Configures a handler-specific redelivery classifier.
    /// </summary>
    public NatsInboundHandlerBuilder WithRedelivery(Action<RedeliveryClassifierBuilder> configure)
    {
        if (configure is null)
        {
            throw new ArgumentNullException(nameof(configure));
        }

        RedeliveryClassifierBuilder builder = new ();
        configure(builder);
        _redeliveryClassifier = ((IBuildable<RedeliveryClassifier>) builder).Build();
        return this;
    }
}
