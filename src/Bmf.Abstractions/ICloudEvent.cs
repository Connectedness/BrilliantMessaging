using System;

namespace Bmf.Abstractions;

/// <summary>
/// Exposes the call-site-owned attributes of a CloudEvent.
/// </summary>
/// <remarks>
/// BMF publishes messages as CloudEvents v1.0 in binary content mode, using the AMQP protocol binding over
/// AMQP 0.9.1. <see cref="Id" /> and <see cref="Time" /> must be created with the message and retained across
/// retries. They must never be regenerated while serializing or publishing.
/// </remarks>
public interface ICloudEvent
{
    /// <summary>
    /// Gets the identifier of the event. Combined with <see cref="CloudEventAttributeNames.Source" /> it uniquely
    /// identifies the event. It is created with the message and must remain stable across retries.
    /// </summary>
    Guid Id { get; }

    /// <summary>
    /// Gets the timestamp at which the event occurred. Like <see cref="Id" />, it is captured when the message is
    /// constructed and must not be regenerated while serializing or publishing.
    /// </summary>
    DateTimeOffset Time { get; }

    /// <summary>
    /// Gets the optional subject of the event in the context of its source, used to identify the particular thing
    /// the event is about (for example the resource the event refers to).
    /// </summary>
    string? Subject { get; }
}
