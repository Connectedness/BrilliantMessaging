using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Abstractions;
using BrilliantMessaging.Core.Messaging.Inbound;

namespace BrilliantMessaging.Transport.Nats.Tests;

public sealed record OrderPlaced : BaseCloudEvent
{
    public string OrderId { get; set; } = string.Empty;
}

public sealed record OrderCancelled : BaseCloudEvent
{
    public string OrderId { get; set; } = string.Empty;
}

public sealed class OrderPlacedHandler : IMessageHandler<OrderPlaced>
{
    public Task HandleAsync(
        OrderPlaced message,
        IncomingMessageContext context,
        CancellationToken cancellationToken = default
    )
    {
        return Task.CompletedTask;
    }
}

public sealed class OrderCancelledHandler : IMessageHandler<OrderCancelled>
{
    public Task HandleAsync(
        OrderCancelled message,
        IncomingMessageContext context,
        CancellationToken cancellationToken = default
    )
    {
        return Task.CompletedTask;
    }
}
