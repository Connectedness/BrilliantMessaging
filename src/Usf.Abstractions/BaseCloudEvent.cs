using System;

namespace Usf.Abstractions;

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
    public Guid Id { get; init; } = UsfUuid.NewId();

    public DateTimeOffset Time { get; init; } = DateTimeOffset.UtcNow;

    public string? Subject { get; init; }
}
