using System;
using BrilliantMessaging.Core.Messaging.Inbound;

namespace BrilliantMessaging.Transport.Nats.Inbound;

/// <summary>
/// Configures the inbound side of a NATS JetStream topology.
/// </summary>
public interface INatsInboundTopologyBuilder : INatsTopologyBuilder<INatsInboundTopologyBuilder>
{
    /// <summary>
    /// Binds a durable pull consumer to a declared stream.
    /// </summary>
    INatsInboundTopologyBuilder Consume(
        string streamName,
        string durableName,
        Action<NatsInboundConsumerBuilder> configure
    );

    /// <summary>
    /// Adds custom middleware to the default inbound pipeline.
    /// </summary>
    INatsInboundTopologyBuilder ConfigureInboundPipeline(Action<MessagePipelineBuilder> configure);

    /// <summary>
    /// Replaces the default message deserialization middleware.
    /// </summary>
    INatsInboundTopologyBuilder UseDeserializationMiddleware<TMiddleware>()
        where TMiddleware : class, IMessageMiddleware;

    /// <summary>
    /// Sets the inbound runtime shutdown timeout.
    /// </summary>
    INatsInboundTopologyBuilder WithShutdownTimeout(TimeSpan shutdownTimeout);

    /// <summary>
    /// Enables or disables periodic JetStream AckProgress heartbeats while messages are in-flight.
    /// </summary>
    INatsInboundTopologyBuilder AckProgress(bool enabled = true);
}
