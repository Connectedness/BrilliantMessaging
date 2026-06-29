namespace BrilliantMessaging.Transport.InMemory.Inbound;

/// <summary>
/// The compiled retry policy for a consumer route: the maximum number of delivery attempts and the backoff that
/// spaces them.
/// </summary>
/// <param name="MaxAttempts">
/// The maximum number of delivery attempts, counting the initial delivery. A value of <c>1</c> means the message
/// is never retried.
/// </param>
/// <param name="Backoff">The backoff strategy applied between attempts.</param>
public sealed record InMemoryRetryPolicy(int MaxAttempts, InMemoryBackoff Backoff);
