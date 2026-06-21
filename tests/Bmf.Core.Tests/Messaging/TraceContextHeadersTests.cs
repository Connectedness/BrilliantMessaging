using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using FluentAssertions;
using Bmf.Core.Messaging;
using Bmf.Core.Messaging.Inbound;
using Xunit;

namespace Bmf.Core.Tests.Messaging;

public sealed class TraceContextHeadersTests
{
    [Fact]
    public void Inject_InjectsCurrentActivityIntoStringHeaders()
    {
        using var activity = new Activity("string-headers").SetIdFormat(ActivityIdFormat.W3C);
        activity.TraceStateString = "vendor=value";
        activity.AddBaggage("tenant", "tenant-7").Start();
        var headers = new Dictionary<string, string?>();

        TraceContextHeaders.Inject(headers);

        headers.Should().ContainKey("traceparent").WhoseValue.Should().Be(activity.Id);
        headers.Should().ContainKey("tracestate").WhoseValue.Should().Be("vendor=value");
        headers.Should().ContainKey("baggage");
        headers["baggage"]!.Replace(" ", string.Empty).Should().Be("tenant=tenant-7");
    }

    [Fact]
    public void Inject_InjectsProvidedActivityIntoObjectHeaders()
    {
        using var activity = new Activity("object-headers")
           .SetIdFormat(ActivityIdFormat.W3C)
           .Start();
        var headers = new Dictionary<string, object?>();

        TraceContextHeaders.Inject(headers, activity);

        headers.Should().ContainKey("traceparent").WhoseValue.Should().Be(activity.Id);
    }

    [Fact]
    public void Inject_DoesNothingWhenNoActivityIsCurrent()
    {
        var previousActivity = Activity.Current;
        Activity.Current = null;

        try
        {
            var stringHeaders = new Dictionary<string, string?>
            {
                ["tenant"] = "tenant-7"
            };
            var objectHeaders = new Dictionary<string, object?>
            {
                ["tenant"] = "tenant-7"
            };

            TraceContextHeaders.Inject(stringHeaders);
            TraceContextHeaders.Inject(objectHeaders);

            stringHeaders.Should().ContainSingle().Which.Should()
               .Be(new KeyValuePair<string, string?>("tenant", "tenant-7"));
            objectHeaders.Should().ContainSingle().Which.Should()
               .Be(new KeyValuePair<string, object?>("tenant", "tenant-7"));
        }
        finally
        {
            Activity.Current = previousActivity;
        }
    }

    [Fact]
    public void Inject_DoesNothingForHierarchicalActivities()
    {
        using var activity = new Activity("hierarchical")
           .SetIdFormat(ActivityIdFormat.Hierarchical)
           .Start();
        var headers = new Dictionary<string, string?>();

        TraceContextHeaders.Inject(headers);

        headers.Should().BeEmpty();
    }

    [Fact]
    public void Extract_ReadsTraceContextAndBaggageFromTransportMessage()
    {
        using var activity = new Activity("extract")
           .SetIdFormat(ActivityIdFormat.W3C)
           .Start();
        activity.TraceStateString = "vendor=value";
        activity.AddBaggage("tenant", "tenant-7");
        var headers = new Dictionary<string, object?>();
        TraceContextHeaders.Inject(headers, activity);

        headers["traceparent"] = Encoding.UTF8.GetBytes((string) headers["traceparent"]!);
        headers["tracestate"] = Encoding.UTF8.GetBytes((string) headers["tracestate"]!);
        headers["baggage"] = Encoding.UTF8.GetBytes((string) headers["baggage"]!);
        var transport = new TestTransportMessage(headers);

        var result = TraceContextHeaders.Extract(transport);

        result.TraceParent.Should().Be(activity.Id);
        result.TraceState.Should().Be("vendor=value");
        result.Baggage.Should().ContainSingle(pair => pair.Key == "tenant" && pair.Value == "tenant-7");
    }

    private sealed class TestTransportMessage : TransportMessage
    {
        public TestTransportMessage(IReadOnlyDictionary<string, object?> headers)
            : base("test", "source", ReadOnlyMemory<byte>.Empty, headers) { }
    }
}
