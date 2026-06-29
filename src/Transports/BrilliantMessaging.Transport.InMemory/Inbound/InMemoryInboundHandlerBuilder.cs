using System;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;

namespace BrilliantMessaging.Transport.InMemory.Inbound;

/// <summary>
/// Fluent builder for a single in-memory handler registration, configuring its deserializer and acknowledgement
/// mode.
/// </summary>
public sealed class InMemoryInboundHandlerBuilder : IBuildable<InMemoryInboundHandlerConfiguration>
{
    private MessageAckMode _ackMode = MessageAckMode.Auto;
    private Type _deserializerType = typeof(PayloadCodecMessageDeserializer);

    /// <inheritdoc />
    InMemoryInboundHandlerConfiguration IBuildable<InMemoryInboundHandlerConfiguration>.Build()
    {
        return new InMemoryInboundHandlerConfiguration(_deserializerType, _ackMode);
    }

    /// <summary>
    /// Overrides the deserializer for this handler with <typeparamref name="TDeserializer" /> instead of the
    /// default payload-codec deserializer.
    /// </summary>
    /// <typeparam name="TDeserializer">The deserializer type to use.</typeparam>
    /// <returns>The same builder for chaining.</returns>
    public InMemoryInboundHandlerBuilder WithDeserializer<TDeserializer>()
        where TDeserializer : class, IMessageDeserializer
    {
        _deserializerType = typeof(TDeserializer);
        return this;
    }

    /// <summary>
    /// Sets the acknowledgement mode for this handler.
    /// </summary>
    /// <param name="ackMode">The acknowledgement mode.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="ackMode" /> is not a defined value.</exception>
    public InMemoryInboundHandlerBuilder WithAckMode(MessageAckMode ackMode)
    {
        if (!Enum.IsDefined(typeof(MessageAckMode), ackMode))
        {
            throw new ArgumentOutOfRangeException(nameof(ackMode), ackMode, "Unsupported acknowledgement mode.");
        }

        _ackMode = ackMode;
        return this;
    }

    /// <summary>
    /// Sets the acknowledgement mode to <see cref="MessageAckMode.Manual" />, so the handler acknowledges the
    /// message itself.
    /// </summary>
    /// <returns>The same builder for chaining.</returns>
    public InMemoryInboundHandlerBuilder ManualAck()
    {
        return WithAckMode(MessageAckMode.Manual);
    }
}
