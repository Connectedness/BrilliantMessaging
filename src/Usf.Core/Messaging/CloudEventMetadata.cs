using System;
using Usf.Abstractions;

namespace Usf.Core.Messaging;

/// <summary>
/// Carries the call-site-owned CloudEvents attributes for a publish operation.
/// </summary>
/// <remarks>
/// <see cref="Id" /> and <see cref="Time" /> must be created at message construction or otherwise supplied by
/// the call site. Serializers and transports must retain them unchanged across retries.
/// </remarks>
public readonly record struct CloudEventMetadata(
    Guid Id,
    DateTimeOffset Time,
    string? Subject = null,
    string? Source = null
)
{
    public static CloudEventMetadata From(ICloudEvent cloudEvent)
    {
        if (cloudEvent is null)
        {
            throw new ArgumentNullException(nameof(cloudEvent));
        }

        return new CloudEventMetadata(cloudEvent.Id, cloudEvent.Time, cloudEvent.Subject);
    }
}
