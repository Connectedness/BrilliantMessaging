using BrilliantMessaging.Abstractions;

namespace BrilliantMessaging.Transport.InMemory.Tests.TestSupport;

/// <summary>
/// A sample CloudEvent used by the in-memory transport tests.
/// </summary>
public sealed record OrderPlaced : BaseCloudEvent
{
    public string OrderId { get; init; } = string.Empty;
}

/// <summary>
/// A second sample CloudEvent used to exercise multi-handler routing.
/// </summary>
public sealed record OrderShipped : BaseCloudEvent
{
    public string OrderId { get; init; } = string.Empty;
}
