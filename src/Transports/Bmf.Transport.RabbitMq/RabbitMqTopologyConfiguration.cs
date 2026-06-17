using System;
using System.Collections.Generic;
using RabbitMQ.Client;
using Bmf.Core.Messaging;
using Bmf.Core.Messaging.Inbound;

using Bmf.Transport.RabbitMq.Inbound;
using Bmf.Transport.RabbitMq.Outbound;

namespace Bmf.Transport.RabbitMq;

/// <summary>
/// The compiled-from configuration of a single RabbitMQ topology. A topology owns one broker connection and can
/// carry both outbound publishing targets and inbound consumers. Outbound-only or consume-only topologies simply
/// leave the corresponding collections empty.
/// </summary>
public sealed record RabbitMqTopologyConfiguration(
    Func<IServiceProvider, ConnectionFactory>? CreateConnectionFactory,
    IReadOnlyList<RabbitMqExchangeDefinition> Exchanges,
    IReadOnlyList<RabbitMqQueueDefinition> Queues,
    IReadOnlyList<RabbitMqBindingDefinition> Bindings,
    IReadOnlyList<RabbitMqOutboundChannelGroupDefinition> OutboundChannelGroups,
    IReadOnlyList<RabbitMqOutboundTargetDefinition> Targets,
    IReadOnlyList<RabbitMqInboundChannelGroupDefinition> InboundChannelGroups,
    IReadOnlyList<RabbitMqInboundConsumerDefinition> Consumers,
    Type DeserializationMiddlewareType,
    Action<MessagePipelineBuilder>? ConfigurePipeline,
    TimeSpan ShutdownTimeout,
    RabbitMqPublisherConfirmMode DefaultPublisherConfirmMode = RabbitMqPublisherConfirmDefaults.Mode,
    TimeSpan? DefaultPublisherConfirmTimeout = null,
    MessageContractRegistry? MessageContractDialect = null
)
{
    /// <summary>
    /// Gets a value indicating whether the configuration defines any inbound consumers (and therefore needs a
    /// topology runtime).
    /// </summary>
    public bool HasInboundEndpoints => Consumers.Count > 0;
}
