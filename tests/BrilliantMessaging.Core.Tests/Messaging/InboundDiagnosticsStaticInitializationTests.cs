using BrilliantMessaging.Core.Messaging.Inbound;
using FluentAssertions;
using Xunit;

namespace BrilliantMessaging.Core.Tests.Messaging;

public sealed class InboundDiagnosticsStaticInitializationTests
{
    [Fact]
    public void InboundDiagnostics_ActivitySourceNameMatchesActivitySourceAndMeter()
    {
        InboundDiagnostics.ActivitySourceName.Should().Be("BrilliantMessaging.Inbound");
        InboundDiagnostics.MeterName.Should().Be("BrilliantMessaging.Inbound");
        InboundDiagnostics.ActivitySource.Name.Should().Be(InboundDiagnostics.ActivitySourceName);
        InboundDiagnostics.Meter.Name.Should().Be(InboundDiagnostics.MeterName);
    }
}
