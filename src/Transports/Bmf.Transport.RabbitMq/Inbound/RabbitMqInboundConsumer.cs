using System;
using System.Collections.Generic;
using Bmf.Core.Messaging.Inbound;

namespace Bmf.Transport.RabbitMq.Inbound;

/// <summary>
/// The compiled, runtime form of a RabbitMQ consumer: the queue, inspector, channel group, body-copy behaviour,
/// and the inbound endpoints it dispatches to.
/// </summary>
public sealed class RabbitMqInboundConsumer
{
    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMqInboundConsumer" /> class.
    /// </summary>
    /// <param name="queueName">The name of the consumed queue.</param>
    /// <param name="inspectorType">The inbound message inspector type; must implement <see cref="IInboundMessageInspector" />.</param>
    /// <param name="copyBody">Whether the delivery body is copied into message-owned memory.</param>
    /// <param name="channelGroup">The channel group the consumer's channels belong to.</param>
    /// <param name="endpoints">The endpoints the consumer dispatches to, keyed by discriminator.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="inspectorType" />, <paramref name="channelGroup" />, or <paramref name="endpoints" /> is <see langword="null" />.</exception>
    /// <exception cref="ArgumentException">Thrown when <paramref name="queueName" /> is null or whitespace, or when <paramref name="inspectorType" /> does not implement <see cref="IInboundMessageInspector" />.</exception>
    public RabbitMqInboundConsumer(
        string queueName,
        Type inspectorType,
        bool copyBody,
        RabbitMqInboundChannelGroup channelGroup,
        IReadOnlyList<RabbitMqInboundEndpoint> endpoints
    )
    {
        QueueName = RequireText(queueName, nameof(queueName));
        InspectorType = inspectorType ?? throw new ArgumentNullException(nameof(inspectorType));
        CopyBody = copyBody;
        ChannelGroup = channelGroup ?? throw new ArgumentNullException(nameof(channelGroup));
        Endpoints = endpoints ?? throw new ArgumentNullException(nameof(endpoints));

        if (!typeof(IInboundMessageInspector).IsAssignableFrom(InspectorType))
        {
            throw new ArgumentException(
                $"Inspector type '{InspectorType}' must implement '{typeof(IInboundMessageInspector)}'.",
                nameof(inspectorType)
            );
        }
    }

    /// <summary>
    /// Gets the name of the consumed queue.
    /// </summary>
    public string QueueName { get; }

    /// <summary>
    /// Gets the inbound message inspector type.
    /// </summary>
    public Type InspectorType { get; }

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

    private static string RequireText(string value, string parameterName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", parameterName);
        }

        return value;
    }
}
