using System;
using System.Collections.Generic;
using BrilliantMessaging.Core.Messaging.Inbound;
using BrilliantMessaging.GuardClauses;

namespace BrilliantMessaging.Transport.RabbitMq.Inbound;

/// <summary>
/// The compiled, runtime form of a RabbitMQ consumer: the queue, inspector chain, channel group, body-copy behaviour,
/// and the inbound endpoints it dispatches to.
/// </summary>
public sealed class RabbitMqInboundConsumer
{
    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMqInboundConsumer" /> class.
    /// </summary>
    /// <param name="queueName">The name of the consumed queue.</param>
    /// <param name="inspectorChain">The compiled inbound message inspector chain.</param>
    /// <param name="copyBody">Whether the delivery body is copied into message-owned memory.</param>
    /// <param name="channelGroup">The channel group the consumer's channels belong to.</param>
    /// <param name="endpoints">The endpoints the consumer dispatches to, keyed by discriminator.</param>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="inspectorChain" />, <paramref name="channelGroup" />, or
    /// <paramref name="endpoints" /> is <see langword="null" />.
    /// </exception>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="queueName" /> is <see langword="null" />.
    /// </exception>
    /// <exception cref="BrilliantMessaging.GuardClauses.Exceptions.EmptyStringException">
    /// Thrown when <paramref name="queueName" /> is empty.
    /// </exception>
    /// <exception cref="BrilliantMessaging.GuardClauses.Exceptions.WhiteSpaceStringException">
    /// Thrown when <paramref name="queueName" /> contains only whitespace.
    /// </exception>
    public RabbitMqInboundConsumer(
        string queueName,
        CompiledInboundMessageInspectorChain inspectorChain,
        bool copyBody,
        RabbitMqInboundChannelGroup channelGroup,
        IReadOnlyList<RabbitMqInboundEndpoint> endpoints
    )
    {
        QueueName = queueName.MustNotBeNullOrWhiteSpace();
        InspectorChain = inspectorChain.MustNotBeNull();
        CopyBody = copyBody;
        ChannelGroup = channelGroup.MustNotBeNull();
        Endpoints = endpoints.MustNotBeNull();
    }

    /// <summary>
    /// Gets the name of the consumed queue.
    /// </summary>
    public string QueueName { get; }

    /// <summary>
    /// Gets the compiled inbound message inspector chain.
    /// </summary>
    public CompiledInboundMessageInspectorChain InspectorChain { get; }

    /// <summary>
    /// Gets a value indicating whether the delivery body is copied into message-owned memory.
    /// </summary>
    public bool CopyBody { get; }

    /// <summary>
    /// Gets the channel group the consumer's channels belong to.
    /// </summary>
    public RabbitMqInboundChannelGroup ChannelGroup { get; }

    /// <summary>
    /// Gets the endpoints the consumer dispatches to.
    /// </summary>
    public IReadOnlyList<RabbitMqInboundEndpoint> Endpoints { get; }
}
