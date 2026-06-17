using System;
using System.Threading;

namespace Bmf.Core.Messaging.Inbound;

/// <summary>
/// Carries the per-message state through the inbound pipeline: the raw transport message, the endpoint that
/// matched, the per-message service provider, the resolved message type and deserialized payload, the
/// acknowledgement handle, and a strongly typed item bag for middleware side-band values.
/// </summary>
public sealed class IncomingMessageContext
{
    /// <summary>
    /// Initializes a new instance of the <see cref="IncomingMessageContext" /> class.
    /// </summary>
    /// <param name="transport">The raw transport message being processed.</param>
    /// <param name="endpoint">The inbound endpoint that matched the message.</param>
    /// <param name="services">The per-message service provider (typically a DI scope).</param>
    /// <param name="acknowledgement">The handle used to acknowledge, reject, or requeue the message.</param>
    /// <param name="cancellationToken">A token signalled when processing should stop.</param>
    /// <param name="messageType">The concrete message type resolved for this delivery.</param>
    /// <param name="items">An optional pre-seeded item bag; when omitted the bag is created lazily on first access.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="transport" />, <paramref name="endpoint" />, <paramref name="services" />, <paramref name="acknowledgement" />, or <paramref name="messageType" /> is <see langword="null" />.</exception>
    public IncomingMessageContext(
        TransportMessage transport,
        InboundEndpoint endpoint,
        IServiceProvider services,
        IMessageAcknowledgement acknowledgement,
        CancellationToken cancellationToken,
        Type messageType,
        IncomingMessageContextItems? items = null
    )
    {
        Transport = transport ?? throw new ArgumentNullException(nameof(transport));
        Endpoint = endpoint ?? throw new ArgumentNullException(nameof(endpoint));
        Services = services ?? throw new ArgumentNullException(nameof(services));
        Acknowledgement = acknowledgement ?? throw new ArgumentNullException(nameof(acknowledgement));
        CancellationToken = cancellationToken;
        MessageType = messageType ?? throw new ArgumentNullException(nameof(messageType));
        Items = items;
    }

    /// <summary>
    /// Gets the raw transport message being processed.
    /// </summary>
    public TransportMessage Transport { get; }

    /// <summary>
    /// Gets the inbound endpoint that matched this message.
    /// </summary>
    public InboundEndpoint Endpoint { get; }

    /// <summary>
    /// Gets the per-message service provider used to resolve handlers and middleware.
    /// </summary>
    public IServiceProvider Services { get; }

    /// <summary>
    /// Gets or sets the deserialized message payload. It is <see langword="null" /> until the deserialization
    /// middleware decodes the transport body.
    /// </summary>
    public object? Message { get; set; }

    /// <summary>
    /// The concrete message type the inspector resolved for this delivery. It may be more derived
    /// than <see cref="InboundEndpoint.MessageType" /> (the endpoint handles any assignable type), and
    /// is the type the deserialization middleware decodes the body into.
    /// </summary>
    public Type MessageType { get; }

    /// <summary>
    /// The strongly-typed side-band values flowing with this message. The backing bag is created on
    /// first access, so a delivery whose inspector contributes nothing and whose pipeline never reads
    /// items allocates none. When an inspector pre-seeds a bag, the context adopts that instance.
    /// </summary>
    public IncomingMessageContextItems Items => field ??= new IncomingMessageContextItems();

    /// <summary>
    /// Gets the handle used to acknowledge, reject, or requeue this message with the transport.
    /// </summary>
    public IMessageAcknowledgement Acknowledgement { get; }

    /// <summary>
    /// Gets the token signalled when message processing should stop.
    /// </summary>
    public CancellationToken CancellationToken { get; }
}
