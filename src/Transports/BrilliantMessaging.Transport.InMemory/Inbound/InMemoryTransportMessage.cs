using System;
using System.Collections.Generic;
using BrilliantMessaging.Core.Messaging.Inbound;

namespace BrilliantMessaging.Transport.InMemory.Inbound;

/// <summary>
/// The in-memory <see cref="TransportMessage" />. It carries the serialized body and the CloudEvents-bound headers
/// of a message routed to a topic. The same type is the unit recorded by <see cref="InMemoryBroker" /> and
/// returned from <see cref="InMemoryBroker.GetMessages" />, so tests can assert the body and headers that reached a
/// topic.
/// </summary>
public sealed class InMemoryTransportMessage : TransportMessage
{
    /// <summary>
    /// Initializes a new instance of the <see cref="InMemoryTransportMessage" /> class.
    /// </summary>
    /// <param name="topic">The topic the message was routed to.</param>
    /// <param name="body">The serialized message body.</param>
    /// <param name="headers">The message headers, including the CloudEvents binary-mode attributes.</param>
    /// <param name="contentType">The body content type, or <see langword="null" /> when unspecified.</param>
    /// <param name="messageId">The transport message identifier, or <see langword="null" /> when unspecified.</param>
    /// <param name="correlationId">The correlation identifier, or <see langword="null" /> when unspecified.</param>
    /// <param name="deliveryAttempt">The one-based delivery attempt.</param>
    public InMemoryTransportMessage(
        string topic,
        ReadOnlyMemory<byte> body,
        IReadOnlyDictionary<string, object?> headers,
        string? contentType = null,
        string? messageId = null,
        string? correlationId = null,
        uint deliveryAttempt = 1
    )
        : base(
            InMemoryInboundEndpoint.TransportNameValue,
            topic,
            body,
            headers,
            contentType,
            contentEncoding: null,
            messageId,
            correlationId,
            replyTo: null,
            timestamp: null,
            priority: null,
            timeToLive: null,
            redelivered: deliveryAttempt > 1,
            deliveryAttempt: deliveryAttempt
        )
    {
        Topic = topic;
    }

    /// <summary>
    /// Gets the topic the message was routed to. This is the same value as <see cref="TransportMessage.Source" />.
    /// </summary>
    public string Topic { get; }
}
