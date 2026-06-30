using System;
using System.Collections.Generic;
using BrilliantMessaging.Transport.InMemory.Inbound;

namespace BrilliantMessaging.Transport.InMemory;

/// <summary>
/// A single queued delivery flowing through a consumer route: the serialized payload plus the one-based attempt
/// counter that drives retry exhaustion. Instances are immutable; a retry produces a new instance with the next
/// attempt number.
/// </summary>
internal sealed class InMemoryDelivery
{
    public InMemoryDelivery(
        string topic,
        ReadOnlyMemory<byte> body,
        IReadOnlyDictionary<string, object?> headers,
        string? contentType,
        string? messageId,
        int attempt
    )
    {
        Topic = topic;
        Body = body;
        Headers = headers;
        ContentType = contentType;
        MessageId = messageId;
        Attempt = attempt;
    }

    public string Topic { get; }

    public ReadOnlyMemory<byte> Body { get; }

    public IReadOnlyDictionary<string, object?> Headers { get; }

    public string? ContentType { get; }

    public string? MessageId { get; }

    /// <summary>
    /// Gets the one-based delivery attempt; the initial delivery is attempt 1.
    /// </summary>
    public int Attempt { get; }

    /// <summary>
    /// Returns a copy of this delivery with the attempt counter advanced by one.
    /// </summary>
    public InMemoryDelivery WithNextAttempt()
    {
        return new InMemoryDelivery(Topic, Body, Headers, ContentType, MessageId, Attempt + 1);
    }

    /// <summary>
    /// Returns a fresh delivery that republishes this payload to another topic, resetting the attempt counter.
    /// </summary>
    public InMemoryDelivery RepublishTo(string topic)
    {
        return new InMemoryDelivery(topic, Body, Headers, ContentType, MessageId, attempt: 1);
    }

    /// <summary>
    /// Materializes the transport message handed to the inbound pipeline (and recorded for inspection).
    /// </summary>
    public InMemoryTransportMessage CreateMessage()
    {
        return new InMemoryTransportMessage(
            Topic,
            Body,
            Headers,
            ContentType,
            MessageId,
            correlationId: null,
            deliveryAttempt: (uint) Attempt
        );
    }
}
