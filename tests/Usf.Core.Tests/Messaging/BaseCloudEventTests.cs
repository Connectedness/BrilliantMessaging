using System;
using FluentAssertions;
using Usf.Abstractions;
using Xunit;

namespace Usf.Core.Tests.Messaging;

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
        UsfUuid.NewId().Should().NotBe(UsfUuid.NewId());
    }

    private sealed record TestCloudEvent : BaseCloudEvent;
}
