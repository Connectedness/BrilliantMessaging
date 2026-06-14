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
        Items = items ?? new IncomingMessageContextItems();
    }

    public TransportMessage Transport { get; }

    public InboundEndpoint Endpoint { get; }

    public IServiceProvider Services { get; }

    public object? Message { get; set; }

    public Type MessageType { get; }

    public IncomingMessageContextItems Items { get; }

    public IMessageAcknowledgement Acknowledgement { get; }

    public CancellationToken CancellationToken { get; }
}
