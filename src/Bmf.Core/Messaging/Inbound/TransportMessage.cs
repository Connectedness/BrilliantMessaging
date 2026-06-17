using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace Bmf.Core.Messaging.Inbound;

/// <summary>
/// The transport-neutral view of a received message: its body, headers, and the common AMQP-style delivery
/// properties. Transport authors derive a concrete message from this base to surface their broker's delivery in
/// a uniform shape the inbound pipeline can consume.
/// </summary>
/// <remarks>
/// The body and headers are stored without defensive copies, so a transport may expose pooled, transport-owned
/// memory through <see cref="Body" /> that is valid only for the duration of the handler. Subclasses are
/// responsible for honouring that contract.
/// </remarks>
public abstract class TransportMessage
{
    /// <summary>
    /// Initializes a new instance of <see cref="TransportMessage" />.
    /// </summary>
    /// <param name="transportName">The transport name.</param>
    /// <param name="source">The transport-specific source.</param>
    /// <param name="body">
    /// The message body. The caller must not mutate its backing memory after construction.
    /// </param>
    /// <param name="headers">
    /// The message headers. The caller must not mutate the dictionary after construction.
    /// </param>
    /// <param name="contentType">The body content type.</param>
    /// <param name="contentEncoding">The body content encoding.</param>
    /// <param name="messageId">The transport message identifier.</param>
    /// <param name="correlationId">The correlation identifier.</param>
    /// <param name="replyTo">The reply destination.</param>
    /// <param name="timestamp">The message timestamp.</param>
    /// <param name="priority">The message priority.</param>
    /// <param name="timeToLive">The message time to live.</param>
    /// <param name="redelivered">Whether the transport reports this message as redelivered.</param>
    /// <param name="deliveryAttempt">The one-based delivery attempt.</param>
    /// <param name="userId">The producer user identifier.</param>
    /// <param name="appId">The producer application identifier.</param>
    /// <remarks>
    /// The body and headers are stored as passed without defensive copies.
    /// </remarks>
    protected TransportMessage(
        string transportName,
        string source,
        ReadOnlyMemory<byte> body,
        IReadOnlyDictionary<string, object?> headers,
        string? contentType = null,
        string? contentEncoding = null,
        string? messageId = null,
        string? correlationId = null,
        string? replyTo = null,
        DateTimeOffset? timestamp = null,
        byte? priority = null,
        TimeSpan? timeToLive = null,
        bool redelivered = false,
        uint deliveryAttempt = 1,
        string? userId = null,
        string? appId = null
    )
    {
        TransportName = RequireText(transportName, nameof(transportName));
        Source = RequireText(source, nameof(source));
        Body = body;
        Headers = headers ?? throw new ArgumentNullException(nameof(headers));
        ContentType = contentType;
        ContentEncoding = contentEncoding;
        MessageId = messageId;
        CorrelationId = correlationId;
        ReplyTo = replyTo;
        Timestamp = timestamp;
        Priority = priority;
        TimeToLive = timeToLive;
        Redelivered = redelivered;
        DeliveryAttempt = deliveryAttempt == 0 ? 1 : deliveryAttempt;
        UserId = userId;
        AppId = appId;
    }

    /// <summary>
    /// Gets the name of the transport that delivered the message.
    /// </summary>
    public string TransportName { get; }

    /// <summary>
    /// Gets the transport-specific source the message was received from (for example a queue name).
    /// </summary>
    public string Source { get; }

    /// <summary>
    /// Gets the message body.
    /// </summary>
    /// <remarks>
    /// A transport may expose transport-owned pooled memory. In that case this memory is valid only until the message
    /// handler completes. The message must not be retained and processing must not be offloaded past the handler's
    /// lifetime; violations read reused buffer contents rather than throwing.
    /// </remarks>
    public ReadOnlyMemory<byte> Body { get; }

    /// <summary>
    /// Gets the content type of the body, or <see langword="null" /> when the transport did not provide one.
    /// </summary>
    public string? ContentType { get; }

    /// <summary>
    /// Gets the content encoding of the body, or <see langword="null" /> when the transport did not provide one.
    /// </summary>
    public string? ContentEncoding { get; }

    /// <summary>
    /// Gets the transport message identifier, or <see langword="null" /> when none was provided.
    /// </summary>
    public string? MessageId { get; }

    /// <summary>
    /// Gets the correlation identifier, or <see langword="null" /> when none was provided.
    /// </summary>
    public string? CorrelationId { get; }

    /// <summary>
    /// Gets the reply destination, or <see langword="null" /> when none was provided.
    /// </summary>
    public string? ReplyTo { get; }

    /// <summary>
    /// Gets the message timestamp, or <see langword="null" /> when none was provided.
    /// </summary>
    public DateTimeOffset? Timestamp { get; }

    /// <summary>
    /// Gets the message priority, or <see langword="null" /> when none was provided.
    /// </summary>
    public byte? Priority { get; }

    /// <summary>
    /// Gets the message time to live, or <see langword="null" /> when none was provided.
    /// </summary>
    public TimeSpan? TimeToLive { get; }

    /// <summary>
    /// Gets a value indicating whether the transport reports this message as a redelivery.
    /// </summary>
    public bool Redelivered { get; }

    /// <summary>
    /// Gets the one-based delivery attempt count.
    /// </summary>
    public uint DeliveryAttempt { get; }

    /// <summary>
    /// Gets the producer user identifier, or <see langword="null" /> when none was provided.
    /// </summary>
    public string? UserId { get; }

    /// <summary>
    /// Gets the producer application identifier, or <see langword="null" /> when none was provided.
    /// </summary>
    public string? AppId { get; }

    /// <summary>
    /// Gets the message headers.
    /// </summary>
    public IReadOnlyDictionary<string, object?> Headers { get; }

    /// <summary>
    /// Attempts to read a header as a string, decoding common transport header representations (string, byte
    /// array, or memory) to text.
    /// </summary>
    /// <param name="name">The header name.</param>
    /// <param name="value">When this method returns, the header value as a string, or <see langword="null" /> when the header is absent or null.</param>
    /// <returns><see langword="true" /> when the header is present and non-null; otherwise <see langword="false" />.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="name" /> is null or whitespace.</exception>
    public bool TryGetHeaderString(string name, out string? value)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(name));
        }

        if (!Headers.TryGetValue(name, out var rawValue) || rawValue is null)
        {
            value = null;
            return false;
        }

        value = rawValue switch
        {
            string stringValue => stringValue,
            byte[] bytes => Encoding.UTF8.GetString(bytes),
            ReadOnlyMemory<byte> memory => Encoding.UTF8.GetString(memory.Span),
            Memory<byte> memory => Encoding.UTF8.GetString(memory.Span),
            IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
            _ => rawValue.ToString()
        };
        return true;
    }

    private static string RequireText(string value, string parameterName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", parameterName);
        }

        return value;
    }
}
