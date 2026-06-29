namespace BrilliantMessaging.Transport.InMemory.Inbound;

/// <summary>
/// The compiled per-consumer delivery policy: an optional retry policy and an optional dead-letter topic. When
/// neither is configured the policy drops failed deliveries.
/// </summary>
/// <param name="Retry">The retry policy, or <see langword="null" /> to drop a failed delivery instead of retrying it.</param>
/// <param name="DeadLetterTopic">
/// The topic exhausted or rejected deliveries are republished to, or <see langword="null" /> to drop them.
/// </param>
public sealed record InMemoryDeliveryPolicy(InMemoryRetryPolicy? Retry, string? DeadLetterTopic)
{
    /// <summary>
    /// Gets the default drop policy: no retry and no dead-letter topic.
    /// </summary>
    public static InMemoryDeliveryPolicy Drop { get; } = new (Retry: null, DeadLetterTopic: null);
}
