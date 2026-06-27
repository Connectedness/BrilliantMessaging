using System.Collections.Immutable;
using BrilliantMessaging.Core.Messaging.Inbound;
using BrilliantMessaging.Transport.RabbitMq;

namespace BrilliantMessaging.Transport.RabbitMq.Inbound;

/// <summary>
/// An immutable declaration of a RabbitMQ consumer on a queue, produced by <see cref="RabbitMqInboundConsumerBuilder" />.
/// </summary>
/// <param name="QueueName">The name of the consumed queue.</param>
/// <param name="InspectorChain">The configured inbound message inspector chain.</param>
/// <param name="ChannelGroupName">The channel group to consume through, or <see langword="null" /> for an implicit group.</param>
/// <param name="ChannelCount">The number of channels the consumer spreads deliveries across.</param>
/// <param name="PrefetchCount">The per-consumer prefetch (QoS) count.</param>
/// <param name="ConsumerDispatchConcurrency">The consumer dispatch concurrency per channel.</param>
/// <param name="CopyBody">Whether the delivery body is copied; <see langword="false" /> uses the transport's pooled buffer.</param>
/// <param name="Handlers">The handler registrations for the consumer.</param>
/// <param name="RedeliveryClassifier">The consumer-wide explicit redelivery classifier, or <see langword="null" /> to use the queue-type default.</param>
/// <param name="QueueType">The explicitly asserted queue type for passive or externally declared queues, or <see langword="null" /> to auto-detect active declarations.</param>
public sealed record RabbitMqInboundConsumerDefinition(
    string QueueName,
    ImmutableArray<InboundMessageInspectorChainEntry> InspectorChain,
    string? ChannelGroupName,
    int ChannelCount,
    ushort PrefetchCount,
    ushort ConsumerDispatchConcurrency,
    bool CopyBody,
    ImmutableArray<RabbitMqInboundHandlerDefinition> Handlers,
    RedeliveryClassifier? RedeliveryClassifier = null,
    RabbitMqQueueType? QueueType = null
);
