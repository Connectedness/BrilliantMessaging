using System;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;

namespace BrilliantMessaging.Transport.RabbitMq.Inbound;

/// <summary>
/// Configures a consume-only RabbitMQ topology. In addition to the shared surface of
/// <see cref="IRabbitMqTopologyBuilder{TSelf}" />, this builder exposes consumers, consumer channel groups,
/// the inbound pipeline, and the shutdown timeout — but no publishing configuration, so a topology
/// configured through this interface cannot accidentally share its connection with publishers. Used by
/// <see cref="RabbitMqTransportModule.AddRabbitMqInboundTopology(BrilliantMessagingBuilder, System.Action{BrilliantMessaging.Transport.RabbitMq.Inbound.IRabbitMqInboundTopologyBuilder})" />
/// .
/// </summary>
public interface IRabbitMqInboundTopologyBuilder : IRabbitMqTopologyBuilder<IRabbitMqInboundTopologyBuilder>
{
    /// <summary>
    /// Configures an inbound consumer channel group.
    /// </summary>
    IRabbitMqInboundTopologyBuilder ChannelGroup(
        string name,
        int maximumChannelCount,
        ushort prefetchCount,
        ushort consumerDispatchConcurrency
    );

    /// <summary>
    /// Configures consumers for the specified queue.
    /// </summary>
    IRabbitMqInboundTopologyBuilder Consume(string queueName, Action<RabbitMqInboundConsumerBuilder> configure);

    /// <summary>
    /// Customizes the inbound message pipeline of this topology.
    /// </summary>
    IRabbitMqInboundTopologyBuilder ConfigureInboundPipeline(Action<MessagePipelineBuilder> configure);

    /// <summary>
    /// Replaces the default deserialization middleware of the inbound pipeline.
    /// </summary>
    IRabbitMqInboundTopologyBuilder UseDeserializationMiddleware<TMiddleware>()
        where TMiddleware : class, IMessageMiddleware;

    /// <summary>
    /// Configures how long consumers may take to drain in-flight messages during shutdown.
    /// </summary>
    IRabbitMqInboundTopologyBuilder WithShutdownTimeout(TimeSpan shutdownTimeout);
}
