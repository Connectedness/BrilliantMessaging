using System;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;

namespace BrilliantMessaging.Transport.RabbitMq.Inbound;

/// <summary>
/// Fluent builder for a single inbound handler registration, configuring its deserializer and acknowledgement
/// mode.
/// </summary>
public sealed class RabbitMqInboundHandlerBuilder : IBuildable<RabbitMqInboundHandlerConfiguration>
{
    private MessageAckMode _ackMode = MessageAckMode.Auto;
    private Type _deserializerType = typeof(PayloadCodecMessageDeserializer);
    private RedeliveryClassifier? _redeliveryClassifier;

    /// <inheritdoc />
    RabbitMqInboundHandlerConfiguration IBuildable<RabbitMqInboundHandlerConfiguration>.Build()
    {
        return new RabbitMqInboundHandlerConfiguration(_deserializerType, _ackMode, _redeliveryClassifier);
    }

    /// <summary>
    /// Overrides the deserializer for this handler with <typeparamref name="TDeserializer" /> instead of the
    /// default payload-codec deserializer.
    /// </summary>
    /// <typeparam name="TDeserializer">The deserializer type to use.</typeparam>
    /// <returns>The same builder for chaining.</returns>
    public RabbitMqInboundHandlerBuilder WithDeserializer<TDeserializer>()
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
    public RabbitMqInboundHandlerBuilder WithAckMode(MessageAckMode ackMode)
    {
        if (!Enum.IsDefined(typeof(MessageAckMode), ackMode))
        {
            throw new ArgumentOutOfRangeException(nameof(ackMode), ackMode, "Unsupported acknowledgement mode.");
        }

        _ackMode = ackMode;
        return this;
    }

    /// <summary>
    /// Configures a redelivery classifier for this handler, overriding the consumer-wide classifier and the
    /// queue-type default.
    /// </summary>
    /// <param name="configure">The callback that configures the classifier.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="configure" /> is <see langword="null" />.</exception>
    public RabbitMqInboundHandlerBuilder WithRedelivery(Action<RedeliveryClassifierBuilder> configure)
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

    /// <summary>
    /// Sets the acknowledgement mode to <see cref="MessageAckMode.Manual" />, so the handler acknowledges
    /// messages itself.
    /// </summary>
    /// <returns>The same builder for chaining.</returns>
    public RabbitMqInboundHandlerBuilder ManualAck()
    {
        return WithAckMode(MessageAckMode.Manual);
    }
}
