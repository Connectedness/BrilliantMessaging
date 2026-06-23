using System;
using FluentAssertions;
using BrilliantMessaging.Abstractions;
using Xunit;

namespace BrilliantMessaging.Core.Tests.Messaging;

public sealed class BaseCloudEventTests
{
    [Fact]
    public void Constructor_CreatesStableNonEmptyIdAndUtcTime()
    {
        var message = new TestCloudEvent();
        var id = message.Id;
        var time = message.Time;

        id.Should().NotBe(Guid.Empty);
        message.Id.Should().Be(id);
        time.Offset.Should().Be(TimeSpan.Zero);
        message.Time.Should().Be(time);
    }

    [Fact]
    public void NewId_CreatesDistinctIdentifiers()
    {
        BrilliantMessagingUuid.NewId().Should().NotBe(BrilliantMessagingUuid.NewId());
    }

    private sealed record TestCloudEvent : BaseCloudEvent;
}
