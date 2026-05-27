using System;
using System.Collections.Generic;
using RabbitMQ.Client;
using Usf.Transport.RabbitMq.Configuration;

namespace Usf.Transport.RabbitMq;

public sealed record RabbitMqPublishingConfiguration(
    Func<IServiceProvider, ConnectionFactory>? ConnectionFactoryFactory,
    RabbitMqChannelPoolingMode ChannelPoolingMode,
    int MaxChannelsPerTarget,
    int SharedChannelPoolSize,
    IReadOnlyList<RabbitMqExchangeDefinition> Exchanges,
    IReadOnlyList<RabbitMqQueueDefinition> Queues,
    IReadOnlyList<RabbitMqBindingDefinition> Bindings,
    IReadOnlyList<RabbitMqPublishRouteConfiguration> Routes
);
