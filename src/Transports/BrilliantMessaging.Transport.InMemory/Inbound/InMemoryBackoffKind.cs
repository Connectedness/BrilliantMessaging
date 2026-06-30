namespace BrilliantMessaging.Transport.InMemory.Inbound;

/// <summary>
/// Identifies how the delay between retry attempts grows.
/// </summary>
public enum InMemoryBackoffKind
{
    /// <summary>
    /// No backoff: each retry is scheduled immediately.
    /// </summary>
    None = 0,

    /// <summary>
    /// Linear backoff: the delay before the <c>n</c>-th retry is the base delay multiplied by <c>n</c>.
    /// </summary>
    Linear = 1,

    /// <summary>
    /// Exponential backoff: the delay before the <c>n</c>-th retry is the base delay multiplied by
    /// <c>2^(n-1)</c>.
    /// </summary>
    Exponential = 2
}
