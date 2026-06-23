using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using BrilliantMessaging.Core.Messaging.Inbound;
using BrilliantMessaging.Core.Messaging.Outbound;
using FluentAssertions;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using Xunit;

namespace BrilliantMessaging.OpenTelemetry.Tests;

public sealed class AddBrilliantMessagingInstrumentationTests
{
    [Fact]
    public void AddBrilliantMessagingInstrumentation_OnTracerProvider_CollectsOutboundAndInboundSpans()
    {
        var exported = new List<Activity>();
        using var provider = Sdk.CreateTracerProviderBuilder()
           .AddBrilliantMessagingInstrumentation()
           .AddInMemoryExporter(exported)
           .Build();

        using (OutboundDiagnostics.ActivitySource.StartActivity("outbound-probe")) { }

        using (InboundDiagnostics.ActivitySource.StartActivity("inbound-probe")) { }

        provider.ForceFlush();

        exported
           .Select(static activity => activity.Source.Name)
           .Should().Contain(OutboundDiagnostics.ActivitySourceName)
           .And.Contain(InboundDiagnostics.ActivitySourceName);
    }

    [Fact]
    public void AddBrilliantMessagingInstrumentation_OnMeterProvider_CollectsOutboundAndInboundInstruments()
    {
        var exported = new List<Metric>();
        using var provider = Sdk
           .CreateMeterProviderBuilder()
           .AddBrilliantMessagingInstrumentation()
           .AddInMemoryExporter(exported)
           .Build();

        OutboundDiagnostics.SentMessages.Add(1);
        OutboundDiagnostics.OperationDuration.Record(0.001);
        InboundDiagnostics.ConsumedMessages.Add(1);

        provider.ForceFlush();

        var instrumentNames = exported.Select(static metric => metric.Name).ToArray();
        instrumentNames.Should().Contain("messaging.client.sent.messages");
        instrumentNames.Should().Contain("messaging.client.consumed.messages");
        instrumentNames.Should().Contain("messaging.client.operation.duration");
    }

    [Fact]
    public void AddBrilliantMessagingInstrumentation_OnNullTracerProviderBuilder_Throws()
    {
        var act = static () => ((TracerProviderBuilder) null!).AddBrilliantMessagingInstrumentation();

        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void AddBrilliantMessagingInstrumentation_OnNullMeterProviderBuilder_Throws()
    {
        var act = static () => ((MeterProviderBuilder) null!).AddBrilliantMessagingInstrumentation();

        act.Should().Throw<ArgumentNullException>();
    }
}
