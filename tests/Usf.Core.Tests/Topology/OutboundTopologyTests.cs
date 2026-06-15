using System;
using System.Collections.Generic;
using FluentAssertions;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Errors;
using Usf.Core.Tests.Messaging.TestSupport;
using Xunit;

namespace Usf.Core.Tests.Topology;

public sealed class OutboundTopologyTests
{
    [Fact]
    public void GetRequiredTarget_SupportsNamedTargetLookup()
    {
        var target = new RecordingTarget<SampleMessage>("named", CloudEventsTestFactory.CreateSerializer());
        var topology = new OutboundTopology(
            new Dictionary<Type, OutboundTarget>
            {
                [typeof(SampleMessage)] = target
            },
            new Dictionary<string, OutboundTarget>(StringComparer.Ordinal)
            {
                ["named"] = target
            }
        );

        var resolvedTarget = topology.GetRequiredTarget("named");
        var typedTarget = topology.GetRequiredTarget<SampleMessage>("named");

        resolvedTarget.Should().BeSameAs(target);
        typedTarget.Should().BeSameAs(target);
    }

    [Fact]
    public void GetRequiredRoutingTarget_ReturnsRoutableTarget()
    {
        var target = new RecordingTarget<SampleMessage>("named", CloudEventsTestFactory.CreateSerializer());
        var topology = new OutboundTopology(
            new Dictionary<Type, OutboundTarget>
            {
                [typeof(SampleMessage)] = target
            },
            new Dictionary<string, OutboundTarget>(StringComparer.Ordinal)
            {
                ["named"] = target
            }
        );

        topology.GetRequiredRoutingTarget<SampleMessage>().Should().BeSameAs(target);
        topology.GetRequiredRoutingTarget<SampleMessage>("named").Should().BeSameAs(target);
    }

    [Fact]
    public void GetRequiredRoutingTarget_ThrowsWhenTargetIsNotRoutable()
    {
        var target = new NonRoutableRecordingTarget<SampleMessage>(
            "named",
            CloudEventsTestFactory.CreateSerializer()
        );
        var topology = new OutboundTopology(
            new Dictionary<Type, OutboundTarget>
            {
                [typeof(SampleMessage)] = target
            },
            new Dictionary<string, OutboundTarget>(StringComparer.Ordinal)
            {
                ["named"] = target
            }
        );

        var byType = () => topology.GetRequiredRoutingTarget<SampleMessage>();
        var byName = () => topology.GetRequiredRoutingTarget<SampleMessage>("named");

        byType.Should().Throw<OutboundTargetNotRoutableException>().Which.MessageType.Should().Be<SampleMessage>();
        byName.Should().Throw<OutboundTargetNotRoutableException>();
    }
}
