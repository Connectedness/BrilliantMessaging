using System;

namespace Bmf.Abstractions;

/// <summary>
/// Provides construction-time defaults for the call-site-owned attributes of a CloudEvent.
/// </summary>
/// <remarks>
/// Constructing a message is the call site supplying its retry-stable <see cref="Id" /> and <see cref="Time" />.
/// These defaults are stable for the lifetime of the message instance and across retries. Generation must not
/// be moved into serialize-time or publish-time code because doing so would turn a retry into a different event.
/// </remarks>
public abstract record BaseCloudEvent : ICloudEvent
{
    /// <summary>
    /// Gets the identifier of the event, defaulting to a fresh time-ordered <see cref="BmfUuid.NewId" /> captured
    /// when the message is constructed. Override the initializer only to adopt an externally supplied id.
    /// </summary>
    public Guid Id { get; init; } = BmfUuid.NewId();

    /// <summary>
    /// Gets the timestamp at which the event occurred, defaulting to <see cref="DateTimeOffset.UtcNow" /> at
    /// construction time.
    /// </summary>
    public DateTimeOffset Time { get; init; } = DateTimeOffset.UtcNow;

    /// <summary>
    /// Gets the optional subject of the event in the context of its source.
    /// </summary>
    public string? Subject { get; init; }
}
