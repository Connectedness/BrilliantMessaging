using System;
using System.Collections.Generic;
using RabbitMQ.Client;
using Usf.Transport.RabbitMq.Configuration;

namespace Usf.Transport.RabbitMq;

public sealed record RabbitMqOutboundTopologyConfiguration(
    Func<IServiceProvider, ConnectionFactory>? CreateConnectionFactory,
    IReadOnlyList<RabbitMqExchangeDefinition> Exchanges,
    IReadOnlyList<RabbitMqQueueDefinition> Queues,
    IReadOnlyList<RabbitMqBindingDefinition> Bindings,
    IReadOnlyList<RabbitMqAddressDefinition> Addresses,
    IReadOnlyList<RabbitMqChannelGroupDefinition> ChannelGroups,
    IReadOnlyList<RabbitMqOutboundTargetDefinition> Targets,
    RabbitMqPublisherConfirmMode DefaultPublisherConfirmMode = RabbitMqPublisherConfirmMode.Confirms,
    TimeSpan? DefaultPublisherConfirmTimeout = null
);
