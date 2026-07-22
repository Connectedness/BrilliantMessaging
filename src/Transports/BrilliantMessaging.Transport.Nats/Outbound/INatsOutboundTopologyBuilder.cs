using System;

namespace BrilliantMessaging.Transport.Nats.Outbound;

/// <summary>
/// Configures the outbound side of a NATS JetStream topology.
/// </summary>
public interface INatsOutboundTopologyBuilder : INatsTopologyBuilder<INatsOutboundTopologyBuilder>
{
    /// <summary>
    /// Maps outbound messages of type <typeparamref name="TMessage" /> to an explicit NATS subject.
    /// </summary>
    INatsOutboundTopologyBuilder Publish<TMessage>(Action<NatsOutboundTargetBuilder<TMessage>> configure);

    /// <summary>
    /// Maps a named outbound target for messages of type <typeparamref name="TMessage" /> to an explicit NATS subject.
    /// </summary>
    INatsOutboundTopologyBuilder PublishNamed<TMessage>(
        string targetName,
        Action<NatsOutboundTargetBuilder<TMessage>> configure
    );
}
