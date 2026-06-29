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

/// <summary>
/// Manually acknowledges the delivery twice to verify duplicate settlement is ignored.
/// </summary>
public sealed class DoubleAckOrderPlacedHandler : IMessageHandler<OrderPlaced>
{
    private readonly HandlerProbe _probe;

    public DoubleAckOrderPlacedHandler(HandlerProbe probe)
    {
        _probe = probe;
    }

    public async Task HandleAsync(
        OrderPlaced message,
        IncomingMessageContext context,
        CancellationToken cancellationToken
    )
    {
        await _probe.HandleAsync(
            context.Transport.Source,
            context.Endpoint.Name,
            message,
            context.Transport.DeliveryAttempt,
            cancellationToken
        ).ConfigureAwait(false);

        await context.Acknowledgement.AckAsync(cancellationToken).ConfigureAwait(false);
        await context.Acknowledgement.AckAsync(cancellationToken).ConfigureAwait(false);
        await context.Acknowledgement.NackAsync(requeue: false, cancellationToken).ConfigureAwait(false);
    }
}

/// <summary>
/// Manually rejects the delivery twice to verify duplicate settlement is ignored.
/// </summary>
public sealed class DoubleNackOrderPlacedHandler : IMessageHandler<OrderPlaced>
{
    private readonly HandlerProbe _probe;

    public DoubleNackOrderPlacedHandler(HandlerProbe probe)
    {
        _probe = probe;
    }

    public async Task HandleAsync(
        OrderPlaced message,
        IncomingMessageContext context,
        CancellationToken cancellationToken
    )
    {
        await _probe.HandleAsync(
            context.Transport.Source,
            context.Endpoint.Name,
            message,
            context.Transport.DeliveryAttempt,
            cancellationToken
        ).ConfigureAwait(false);

        await context.Acknowledgement.NackAsync(requeue: false, cancellationToken).ConfigureAwait(false);
        await context.Acknowledgement.NackAsync(requeue: false, cancellationToken).ConfigureAwait(false);
    }
}
