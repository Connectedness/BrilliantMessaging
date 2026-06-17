using System.Collections.Generic;

namespace Bmf.Core.Messaging.Outbound;

/// <summary>
/// A transport-neutral, ready-to-publish message: the serialized body together with the metadata a transport
/// needs to bind it to the wire (content type and encoding, headers, and message/correlation identifiers).
/// </summary>
/// <param name="Body">The serialized message body.</param>
/// <param name="ContentType">The content type of the body, or <see langword="null" /> when unspecified.</param>
/// <param name="ContentEncoding">The content encoding of the body, or <see langword="null" /> when unspecified.</param>
/// <param name="Headers">The headers to attach to the message (for example bound CloudEvents attributes).</param>
/// <param name="MessageId">The transport message identifier, or <see langword="null" /> when unspecified.</param>
/// <param name="CorrelationId">The correlation identifier, or <see langword="null" /> when unspecified.</param>
public readonly record struct SerializedMessage(
    byte[] Body,
    string? ContentType,
    string? ContentEncoding,
    IReadOnlyDictionary<string, string?> Headers,
    string? MessageId,
    string? CorrelationId
);
