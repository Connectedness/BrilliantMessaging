using System;
using BrilliantMessaging.Abstractions;

namespace BrilliantMessaging.Transport.RabbitMq.Tests.TestSupport;

public sealed record ValidationMessageB(string Value) : ICloudEvent
{
    Guid ICloudEvent.Id { get; } = BrilliantMessagingUuid.NewId();

    DateTimeOffset ICloudEvent.Time { get; } = DateTimeOffset.UtcNow;

    string? ICloudEvent.Subject => null;
}
