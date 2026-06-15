using System;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Serialization;

namespace Usf.Transport.RabbitMq;

public sealed class RabbitMqInboundHandlerBuilder
{
    private MessageAckMode _ackMode = MessageAckMode.Auto;
    private Type _deserializerType = typeof(PayloadCodecMessageDeserializer);

    public RabbitMqInboundHandlerBuilder WithDeserializer<TDeserializer>()
        where TDeserializer : class, IMessageDeserializer
    {
        _deserializerType = typeof(TDeserializer);
        return this;
    }

    public RabbitMqInboundHandlerBuilder WithAckMode(MessageAckMode ackMode)
    {
        if (!Enum.IsDefined(typeof(MessageAckMode), ackMode))
        {
            throw new ArgumentOutOfRangeException(nameof(ackMode), ackMode, "Unsupported acknowledgement mode.");
        }

        _ackMode = ackMode;
        return this;
    }

    public RabbitMqInboundHandlerBuilder ManualAck()
    {
        return WithAckMode(MessageAckMode.Manual);
    }

    internal (Type DeserializerType, MessageAckMode AckMode) Build()
    {
        return (_deserializerType, _ackMode);
    }
}
