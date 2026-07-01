using System;

namespace BrilliantMessaging.Transport.InMemory;

/// <summary>
/// Configures how an in-memory topology records routed messages for later inspection through
/// <see cref="InMemoryBroker.GetMessages" />.
/// </summary>
public readonly record struct InMemoryRecordingOptions
{
    private InMemoryRecordingOptions(bool enabled, int? maxPerTopic)
    {
        Enabled = enabled;
        MaxPerTopic = maxPerTopic;
    }

    /// <summary>
    /// Gets the option that disables message recording.
    /// </summary>
    public static InMemoryRecordingOptions Off { get; } = new (enabled: false, maxPerTopic: null);

    /// <summary>
    /// Gets the option that records every routed message without a per-topic bound.
    /// </summary>
    public static InMemoryRecordingOptions Unbounded { get; } = new (enabled: true, maxPerTopic: null);

    /// <summary>
    /// Gets a value indicating whether routed messages are recorded.
    /// </summary>
    public bool Enabled { get; }

    /// <summary>
    /// Gets the maximum number of recorded messages retained per topic, or <see langword="null" /> when recording
    /// is unbounded.
    /// </summary>
    public int? MaxPerTopic { get; }

    /// <summary>
    /// Creates an option that records at most <paramref name="maxPerTopic" /> messages per topic.
    /// </summary>
    /// <param name="maxPerTopic">The maximum number of recorded messages retained per topic. The value must be positive.</param>
    /// <returns>The bounded recording option.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="maxPerTopic" /> is not positive.</exception>
    public static InMemoryRecordingOptions Bounded(int maxPerTopic)
    {
        if (maxPerTopic <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxPerTopic),
                maxPerTopic,
                "The value must be positive."
            );
        }

        return new InMemoryRecordingOptions(enabled: true, maxPerTopic);
    }
}
