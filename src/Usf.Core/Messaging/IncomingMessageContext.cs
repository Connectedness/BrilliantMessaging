using System;
using System.Threading;

namespace Usf.Core.Messaging;

public sealed class IncomingMessageContext
{
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

    public TransportMessage Transport { get; }

    public InboundEndpoint Endpoint { get; }

    public IServiceProvider Services { get; }

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

    public IMessageAcknowledgement Acknowledgement { get; }

    public CancellationToken CancellationToken { get; }
}
