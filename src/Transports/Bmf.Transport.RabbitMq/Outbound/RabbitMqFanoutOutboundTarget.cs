using Bmf.Core.Messaging;
using Bmf.Core.Messaging.Outbound;

namespace Bmf.Transport.RabbitMq.Outbound;

/// <summary>
/// A RabbitMQ outbound target for a fanout exchange, broadcasting each message to all bound queues.
/// </summary>
/// <typeparam name="TMessage">The message type the target publishes.</typeparam>
public sealed class RabbitMqFanoutOutboundTarget<TMessage> : RabbitMqOutboundTarget<TMessage>
{
    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMqFanoutOutboundTarget{TMessage}" /> class.
    /// </summary>
    /// <param name="name">The logical name of the target.</param>
    /// <param name="serializer">The serializer used to turn messages into CloudEvents envelopes.</param>
    /// <param name="messageContractRegistry">The registry used to resolve discriminators and data schemas.</param>
    /// <param name="topologyName">The name of the topology the target belongs to.</param>
    /// <param name="channelGroup">The channel group that supplies publish channels.</param>
    /// <param name="exchangeName">The name of the fanout exchange.</param>
    /// <param name="isMandatory">Whether published messages are sent with the AMQP mandatory flag.</param>
    public RabbitMqFanoutOutboundTarget(
        string name,
        IMessageSerializer serializer,
        IMessageContractRegistry messageContractRegistry,
        string topologyName,
        RabbitMqOutboundChannelGroup channelGroup,
        string exchangeName,
        bool isMandatory
    )
        : base(name, serializer, messageContractRegistry, topologyName, channelGroup, exchangeName, isMandatory) { }
}
