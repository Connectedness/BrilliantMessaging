using System.Collections.Generic;
using BrilliantMessaging.Core.Messaging.Inbound;

namespace BrilliantMessaging.Transport.Nats.Inbound;

/// <summary>
/// NATS transport message projection used by the shared inbound pipeline.
/// </summary>
public sealed class NatsTransportMessage : TransportMessage
{
    /// <summary>
    /// Initializes a new instance of the <see cref="NatsTransportMessage" /> class.
    /// </summary>
    public NatsTransportMessage(
        string subject,
        byte[] body,
        IReadOnlyDictionary<string, object?> headers,
        string? contentType,
        string? contentEncoding,
        string? messageId,
        string? correlationId,
        uint deliveryAttempt
    ) : base(
        NatsTopology.TransportNameValue,
        subject,
        body,
        headers,
        contentType,
        contentEncoding,
        messageId,
        correlationId,
        deliveryAttempt: deliveryAttempt > 1 ? deliveryAttempt : 1,
        redelivered: deliveryAttempt > 1
    ) { }

    /// <inheritdoc />
    public override string MessagingSystem => "nats";
}
