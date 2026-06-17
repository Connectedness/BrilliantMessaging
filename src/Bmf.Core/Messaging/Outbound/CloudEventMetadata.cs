using System;
using Bmf.Abstractions;

namespace Bmf.Core.Messaging.Outbound;

/// <summary>
/// Carries the call-site-owned CloudEvents attributes for a publish operation.
/// </summary>
/// <remarks>
/// <see cref="Id" /> and <see cref="Time" /> must be created at message construction or otherwise supplied by
/// the call site. Serializers and transports must retain them unchanged across retries.
/// </remarks>
/// <param name="Id">The retry-stable event identifier created at message construction.</param>
/// <param name="Time">The retry-stable event timestamp created at message construction.</param>
/// <param name="Subject">The optional subject of the event, or <see langword="null" />.</param>
/// <param name="Source">The optional per-call source override, or <see langword="null" /> to use the configured <see cref="CloudEventsOptions.Source" />.</param>
public readonly record struct CloudEventMetadata(
    Guid Id,
    DateTimeOffset Time,
    string? Subject = null,
    string? Source = null
)
{
    /// <summary>
    /// Creates a <see cref="CloudEventMetadata" /> from the call-site-owned attributes of an
    /// <see cref="ICloudEvent" />.
    /// </summary>
    /// <param name="cloudEvent">The cloud event to read the id, time, and subject from.</param>
    /// <returns>The metadata derived from <paramref name="cloudEvent" />.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="cloudEvent" /> is <see langword="null" />.</exception>
    public static CloudEventMetadata From(ICloudEvent cloudEvent)
    {
        if (cloudEvent is null)
        {
            throw new ArgumentNullException(nameof(cloudEvent));
        }

        return new CloudEventMetadata(cloudEvent.Id, cloudEvent.Time, cloudEvent.Subject);
    }
}
