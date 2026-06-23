using System;
using BrilliantMessaging.Abstractions;

namespace BrilliantMessaging.Core.Tests.Messaging.TestSupport;

public sealed record SampleMessage(string Value) : ICloudEvent
{
    Guid ICloudEvent.Id { get; } = BrilliantMessagingUuid.NewId();

    DateTimeOffset ICloudEvent.Time { get; } = DateTimeOffset.UtcNow;

    string? ICloudEvent.Subject => null;
}
