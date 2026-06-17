using System;
using System.Collections.Generic;

namespace Bmf.Transport.RabbitMq.Inbound;

/// <summary>
/// An immutable declaration of a RabbitMQ consumer on a queue, produced by <see cref="RabbitMqInboundConsumerBuilder" />.
/// </summary>
/// <param name="QueueName">The name of the consumed queue.</param>
/// <param name="InspectorType">The inbound message inspector type.</param>
/// <param name="ChannelGroupName">The channel group to consume through, or <see langword="null" /> for an implicit group.</param>
/// <param name="ChannelCount">The number of channels the consumer spreads deliveries across.</param>
/// <param name="PrefetchCount">The per-consumer prefetch (QoS) count.</param>
/// <param name="ConsumerDispatchConcurrency">The consumer dispatch concurrency per channel.</param>
/// <param name="CopyBody">Whether the delivery body is copied; <see langword="false" /> uses the transport's pooled buffer.</param>
/// <param name="Handlers">The handler registrations for the consumer.</param>
public sealed record RabbitMqInboundConsumerDefinition(
    string QueueName,
    Type InspectorType,
    string? ChannelGroupName,
    int ChannelCount,
    ushort PrefetchCount,
    ushort ConsumerDispatchConcurrency,
    bool CopyBody,
    IReadOnlyList<RabbitMqInboundHandlerDefinition> Handlers
);
