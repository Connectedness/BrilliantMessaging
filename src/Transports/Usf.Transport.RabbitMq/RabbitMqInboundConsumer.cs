using System;
using System.Collections.Generic;
using Usf.Core.Messaging;

namespace Usf.Transport.RabbitMq;

public sealed class RabbitMqInboundConsumer
{
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

    public string QueueName { get; }

    public Type InspectorType { get; }

    public bool CopyBody { get; }

    public RabbitMqInboundChannelGroup ChannelGroup { get; }

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
