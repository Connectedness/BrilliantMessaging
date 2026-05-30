using System;

namespace Usf.Transport.RabbitMq.Configuration;

public sealed record RabbitMqChannelGroupDefinition(
    string Name,
    int MaximumChannelCount,
    RabbitMqPublisherConfirmMode PublisherConfirmMode = RabbitMqPublisherConfirmMode.Confirms,
    TimeSpan? PublisherConfirmTimeout = null
);
