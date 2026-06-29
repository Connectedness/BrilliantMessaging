using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging.Inbound;

namespace BrilliantMessaging.Transport.InMemory.Tests.TestSupport;

/// <summary>
/// Records every <see cref="OrderPlaced" /> delivery through the shared <see cref="HandlerProbe" />.
/// </summary>
public sealed class OrderPlacedHandler : IMessageHandler<OrderPlaced>
{
    private readonly HandlerProbe _probe;

    public OrderPlacedHandler(HandlerProbe probe)
    {
        _probe = probe;
    }

    public Task HandleAsync(OrderPlaced message, IncomingMessageContext context, CancellationToken cancellationToken)
    {
        return _probe.HandleAsync(
            context.Transport.Source,
            context.Endpoint.Name,
            message,
            context.Transport.DeliveryAttempt,
            cancellationToken
        );
    }
}

/// <summary>
/// A second <see cref="OrderPlaced" /> handler, used to exercise fanout to multiple consumer routes on a topic.
/// </summary>
public sealed class SecondOrderPlacedHandler : IMessageHandler<OrderPlaced>
{
    private readonly HandlerProbe _probe;

    public SecondOrderPlacedHandler(HandlerProbe probe)
    {
        _probe = probe;
    }

    public Task HandleAsync(OrderPlaced message, IncomingMessageContext context, CancellationToken cancellationToken)
    {
        return _probe.HandleAsync(
            context.Transport.Source,
            context.Endpoint.Name,
            message,
            context.Transport.DeliveryAttempt,
            cancellationToken
        );
    }
}

/// <summary>
/// Records every <see cref="OrderShipped" /> delivery through the shared <see cref="HandlerProbe" />.
/// </summary>
public sealed class OrderShippedHandler : IMessageHandler<OrderShipped>
{
    private readonly HandlerProbe _probe;

    public OrderShippedHandler(HandlerProbe probe)
    {
        _probe = probe;
    }

    public Task HandleAsync(OrderShipped message, IncomingMessageContext context, CancellationToken cancellationToken)
    {
        return _probe.HandleAsync(
            context.Transport.Source,
            context.Endpoint.Name,
            message,
            context.Transport.DeliveryAttempt,
            cancellationToken
        );
    }
}
