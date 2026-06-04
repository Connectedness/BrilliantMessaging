using System;

namespace Usf.Abstractions;

/// <summary>
/// Exposes the call-site-owned attributes of a CloudEvent.
/// </summary>
/// <remarks>
/// USF publishes messages as CloudEvents v1.0 in binary content mode, using the AMQP protocol binding over
/// AMQP 0.9.1. <see cref="Id" /> and <see cref="Time" /> must be created with the message and retained across
/// retries. They must never be regenerated while serializing or publishing.
/// </remarks>
public interface ICloudEvent
{
    Guid Id { get; }

    DateTimeOffset Time { get; }

    string? Subject { get; }
}
