using System;
using Bmf.Core.Messaging;
using Bmf.Core.Messaging.Outbound;

namespace Bmf.Transport.RabbitMq.Outbound;

/// <summary>
/// A RabbitMQ outbound target for a topic exchange, routing on a pattern routing-key match.
/// </summary>
/// <typeparam name="TMessage">The message type the target publishes.</typeparam>
public sealed class RabbitMqTopicOutboundTarget<TMessage> : RabbitMqRoutingKeyOutboundTarget<TMessage>
{
    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMqTopicOutboundTarget{TMessage}" /> class.
    /// </summary>
    /// <param name="name">The logical name of the target.</param>
    /// <param name="serializer">The serializer used to turn messages into CloudEvents envelopes.</param>
    /// <param name="messageContractRegistry">The registry used to resolve discriminators and data schemas.</param>
    /// <param name="topologyName">The name of the topology the target belongs to.</param>
    /// <param name="channelGroup">The channel group that supplies publish channels.</param>
    /// <param name="exchangeName">The name of the topic exchange.</param>
    /// <param name="isMandatory">Whether published messages are sent with the AMQP mandatory flag.</param>
    /// <param name="constantRoutingKey">A fixed topic routing key, or <see langword="null" /> when a factory is used.</param>
    /// <param name="routingKeyFactory">A per-message routing-key factory, or <see langword="null" /> when a fixed key is used.</param>
    public RabbitMqTopicOutboundTarget(
        string name,
        IMessageSerializer serializer,
        IMessageContractRegistry messageContractRegistry,
        string topologyName,
        RabbitMqOutboundChannelGroup channelGroup,
        string exchangeName,
        bool isMandatory,
        string? constantRoutingKey,
        Func<TMessage, string>? routingKeyFactory
    )
        : base(
            name,
            serializer,
            messageContractRegistry,
            topologyName,
            channelGroup,
            exchangeName,
            isMandatory,
            constantRoutingKey,
            routingKeyFactory
        ) { }
}
