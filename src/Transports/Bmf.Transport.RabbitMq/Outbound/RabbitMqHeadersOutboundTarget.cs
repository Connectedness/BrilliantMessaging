using System;
using System.Collections.Generic;
using Bmf.Core.Messaging;
using Bmf.Core.Messaging.Outbound;

namespace Bmf.Transport.RabbitMq.Outbound;

/// <summary>
/// A RabbitMQ outbound target for a headers exchange. It attaches the configured headers to every published
/// message so the broker can route on header matches rather than a routing key.
/// </summary>
/// <typeparam name="TMessage">The message type the target publishes.</typeparam>
public sealed class RabbitMqHeadersOutboundTarget<TMessage> : RabbitMqOutboundTarget<TMessage>
{
    private readonly IReadOnlyDictionary<string, object?> _headers;

    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMqHeadersOutboundTarget{TMessage}" /> class.
    /// </summary>
    /// <param name="name">The logical name of the target.</param>
    /// <param name="serializer">The serializer used to turn messages into CloudEvents envelopes.</param>
    /// <param name="messageContractRegistry">The registry used to resolve discriminators and data schemas.</param>
    /// <param name="topologyName">The name of the topology the target belongs to.</param>
    /// <param name="channelGroup">The channel group that supplies publish channels.</param>
    /// <param name="exchangeName">The name of the headers exchange.</param>
    /// <param name="isMandatory">Whether published messages are sent with the AMQP mandatory flag.</param>
    /// <param name="headers">The headers attached to every message and used for routing.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="headers" /> is <see langword="null" />.</exception>
    public RabbitMqHeadersOutboundTarget(
        string name,
        IMessageSerializer serializer,
        IMessageContractRegistry messageContractRegistry,
        string topologyName,
        RabbitMqOutboundChannelGroup channelGroup,
        string exchangeName,
        bool isMandatory,
        IReadOnlyDictionary<string, object?> headers
    )
        : base(name, serializer, messageContractRegistry, topologyName, channelGroup, exchangeName, isMandatory)
    {
        _headers = headers ?? throw new ArgumentNullException(nameof(headers));
    }

    /// <inheritdoc />
    protected override IReadOnlyDictionary<string, object?> GetRawRouteHeaders()
    {
        return _headers;
    }

    /// <inheritdoc />
    protected override IReadOnlyDictionary<string, object?> GetRouteHeaders(TMessage message)
    {
        return _headers;
    }
}
