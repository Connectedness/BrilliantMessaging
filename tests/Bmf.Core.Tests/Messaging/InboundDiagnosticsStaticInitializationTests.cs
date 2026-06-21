using Bmf.Core.Messaging.Inbound;
using FluentAssertions;
using Xunit;

namespace Bmf.Core.Tests.Messaging;

public sealed class InboundDiagnosticsStaticInitializationTests
{
    [Fact]
    public void InboundDiagnostics_ActivitySourceNameMatchesActivitySourceAndMeter()
    {
        InboundDiagnostics.ActivitySourceName.Should().Be("Bmf.Inbound");
        InboundDiagnostics.MeterName.Should().Be("Bmf.Inbound");
        InboundDiagnostics.ActivitySource.Name.Should().Be(InboundDiagnostics.ActivitySourceName);
        InboundDiagnostics.Meter.Name.Should().Be(InboundDiagnostics.MeterName);
    }
}
