using System;
using BrilliantMessaging.Core.Messaging;

namespace BrilliantMessaging.Transport.RabbitMq.Outbound;

/// <summary>
/// Configures a publish-only RabbitMQ topology. In addition to the shared surface of
/// <see cref="IRabbitMqTopologyBuilder{TSelf}" />, this builder exposes outbound targets,
/// publisher channel groups, and publisher-confirm defaults — but no consumer configuration, so a topology
/// configured through this interface cannot accidentally share its connection with consumers. Used by
/// <see cref="RabbitMqTransportModule.AddRabbitMqOutboundTopology(BrilliantMessagingBuilder, System.Action{BrilliantMessaging.Transport.RabbitMq.Outbound.IRabbitMqOutboundTopologyBuilder})" />
/// .
/// </summary>
public interface IRabbitMqOutboundTopologyBuilder : IRabbitMqTopologyBuilder<IRabbitMqOutboundTopologyBuilder>
{
    /// <summary>
    /// Configures the default outbound target for <typeparamref name="TMessage" />.
    /// </summary>
    IRabbitMqOutboundTopologyBuilder Publish<TMessage>(Action<RabbitMqOutboundTargetBuilder<TMessage>> configure);

    /// <summary>
    /// Configures a named outbound target for <typeparamref name="TMessage" />.
    /// </summary>
    IRabbitMqOutboundTopologyBuilder PublishNamed<TMessage>(
        string targetName,
        Action<RabbitMqOutboundTargetBuilder<TMessage>> configure
    );

    /// <summary>
    /// Configures an outbound publisher channel group.
    /// </summary>
    IRabbitMqOutboundTopologyBuilder ChannelGroup(
        string name,
        int maximumChannelCount,
        RabbitMqPublisherConfirmMode? publisherConfirmMode = null,
        TimeSpan? publisherConfirmTimeout = null
    );

    /// <summary>
    /// Configures the publisher confirm mode used by channel groups that do not specify their own.
    /// </summary>
    IRabbitMqOutboundTopologyBuilder WithDefaultPublisherConfirmMode(
        RabbitMqPublisherConfirmMode publisherConfirmMode
    );

    /// <summary>
    /// Configures the publisher confirm timeout used by channel groups that do not specify their own.
    /// </summary>
    IRabbitMqOutboundTopologyBuilder WithDefaultPublisherConfirmTimeout(TimeSpan publisherConfirmTimeout);
}
