using System;
using BrilliantMessaging.Abstractions;

namespace BrilliantMessaging.Transport.RabbitMq.Tests.TestSupport;

public sealed record RabbitMqAuditMessage(int Id, string EventName) : ICloudEvent
{
    Guid ICloudEvent.Id { get; } = BrilliantMessagingUuid.NewId();

    DateTimeOffset ICloudEvent.Time { get; } = DateTimeOffset.UtcNow;

    string? ICloudEvent.Subject => null;
}
