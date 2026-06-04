using System;
using Usf.Abstractions;

namespace Usf.Transport.RabbitMq.Tests.TestSupport;

public sealed record RabbitMqAuditMessage(int Id, string EventName) : ICloudEvent
{
    Guid ICloudEvent.Id { get; } = UsfUuid.NewId();

    DateTimeOffset ICloudEvent.Time { get; } = DateTimeOffset.UtcNow;

    string? ICloudEvent.Subject => null;
}
