using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Bmf.Core.Messaging;
using Bmf.Core.Messaging.Inbound;
using Bmf.Core.Messaging.Outbound;
using Bmf.Core.Tests.Messaging.TestSupport;
using FluentAssertions;
using Xunit;

namespace Bmf.Core.Tests.Messaging;

public sealed class TopologyTests
{
    [Fact]
    public void GetRequiredTarget_SupportsNamedTargetLookup()
    {
        var target = new RecordingTarget<SampleMessage>("named", CloudEventsTestFactory.CreateSerializer());
        var topology = new TestTopology(
            Topology.DefaultName,
            new Dictionary<Type, OutboundTarget>
            {
                [typeof(SampleMessage)] = target
            },
            new Dictionary<string, OutboundTarget>(StringComparer.Ordinal)
            {
                ["named"] = target
            },
            new Dictionary<string, InboundEndpoint>(StringComparer.Ordinal)
        );

        var resolvedTarget = topology.GetRequiredTarget("named");
        var typedTarget = topology.GetRequiredTarget<SampleMessage>("named");

        resolvedTarget.Should().BeSameAs(target);
        typedTarget.Should().BeSameAs(target);
    }

    [Fact]
    public void Constructor_RejectsInvalidName()
    {
        var act = () => new TestTopology("   ");

        act.Should().Throw<ArgumentException>().WithParameterName("name");
    }

    [Fact]
    public void Lookups_UseOrdinalNameComparison()
    {
        var target = new RecordingTarget<SampleMessage>("Named", CloudEventsTestFactory.CreateSerializer());
        var topology = new TestTopology(
            Topology.DefaultName,
            targetsByName: new Dictionary<string, OutboundTarget>(StringComparer.Ordinal)
            {
                ["Named"] = target
            }
        );

        var act = () => topology.GetRequiredTarget("named");

        act.Should().Throw<OutboundTargetNotFoundException>();
    }

    [Fact]
    public void StateProperties_DescribeEmptyTopology()
    {
        var topology = new TestTopology(Topology.DefaultName);

        topology.IsEmpty.Should().BeTrue();
        topology.IsOutboundOnly.Should().BeFalse();
        topology.IsInboundOnly.Should().BeFalse();
    }

    [Fact]
    public void StateProperties_DescribeOutboundOnlyTopology()
    {
        var target = new RecordingTarget<SampleMessage>("target", CloudEventsTestFactory.CreateSerializer());
        var topology = new TestTopology(
            Topology.DefaultName,
            targetsByMessageType: new Dictionary<Type, OutboundTarget>
            {
                [typeof(SampleMessage)] = target
            }
        );

        topology.IsEmpty.Should().BeFalse();
        topology.IsOutboundOnly.Should().BeTrue();
        topology.IsInboundOnly.Should().BeFalse();
    }

    [Fact]
    public void StateProperties_DescribeInboundOnlyTopology()
    {
        var endpoint = new InboundEndpoint<SampleMessage>(
            "endpoint",
            "test",
            Topology.DefaultName,
            typeof(SampleMessageHandler),
            typeof(PayloadCodecMessageDeserializer),
            "sample",
            MessageHandlerInvocation.Create<SampleMessage, SampleMessageHandler>()
        );
        var topology = new TestTopology(
            Topology.DefaultName,
            endpointsByName: new Dictionary<string, InboundEndpoint>(StringComparer.Ordinal)
            {
                [endpoint.Name] = endpoint
            }
        );

        topology.IsEmpty.Should().BeFalse();
        topology.IsOutboundOnly.Should().BeFalse();
        topology.IsInboundOnly.Should().BeTrue();
    }

    [Fact]
    public void RegistrationCatalog_RejectsInvalidAndDuplicateNamesWithOrdinalComparison()
    {
        TopologyRegistrationCatalog catalog = new ();
        catalog.Add("orders");
        catalog.Add("ORDERS");

        var invalidAction = () => catalog.Add(" ");
        var duplicateAction = () => catalog.Add("orders");

        invalidAction.Should().Throw<ArgumentException>().WithParameterName("name");
        duplicateAction.Should().Throw<InvalidOperationException>()
           .WithMessage("Topology 'orders' is already registered. Registered topologies: ORDERS, orders.");
    }

    [Fact]
    public void GetRequiredRoutingTarget_ReturnsRoutableTarget()
    {
        var target = new RecordingTarget<SampleMessage>("named", CloudEventsTestFactory.CreateSerializer());
        var topology = new TestTopology(
            Topology.DefaultName,
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
        var topology = new TestTopology(
            Topology.DefaultName,
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

    [Fact]
    public void TargetLookups_ReturnFalseOrThrowForMissingTargets()
    {
        var topology = new TestTopology(Topology.DefaultName);

        var requiredByType = () => topology.GetRequiredTarget<SampleMessage>();
        var requiredByName = () => topology.GetRequiredTarget("missing");

        topology.TryGetTarget(typeof(SampleMessage), out var targetByType).Should().BeFalse();
        targetByType.Should().BeNull();
        topology.TryGetTarget("missing", out var targetByName).Should().BeFalse();
        targetByName.Should().BeNull();
        requiredByType.Should().Throw<OutboundTargetNotFoundException>();
        requiredByName.Should().Throw<OutboundTargetNotFoundException>();
    }

    [Fact]
    public void TargetLookups_RejectNullOrBlankInputs()
    {
        var topology = new TestTopology(Topology.DefaultName);

        var requiredByNullType = () => topology.GetRequiredTarget((Type) null!);
        var tryByNullType = () => topology.TryGetTarget((Type) null!, out _);
        var requiredByBlankName = () => topology.GetRequiredTarget(" ");
        var tryByBlankName = () => topology.TryGetTarget(" ", out _);

        requiredByNullType.Should().Throw<ArgumentNullException>().WithParameterName("messageType");
        tryByNullType.Should().Throw<ArgumentNullException>().WithParameterName("messageType");
        requiredByBlankName.Should().Throw<ArgumentException>().WithParameterName("name");
        tryByBlankName.Should().Throw<ArgumentException>().WithParameterName("name");
    }

    [Fact]
    public void TargetLookups_ThrowWhenNamedTargetHasDifferentMessageType()
    {
        var target = new RecordingTarget<OtherMessage>("named", CloudEventsTestFactory.CreateSerializer());
        var topology = new TestTopology(
            Topology.DefaultName,
            targetsByName: new Dictionary<string, OutboundTarget>(StringComparer.Ordinal)
            {
                ["named"] = target
            }
        );

        var act = () => topology.GetRequiredTarget<SampleMessage>("named");

        act.Should().Throw<OutboundTargetTypeMismatchException>()
           .Which.ExpectedMessageType.Should().Be(typeof(OtherMessage));
    }

    [Fact]
    public void EndpointLookups_ReturnEndpointOrReportMissingEndpoint()
    {
        var endpoint = new InboundEndpoint<SampleMessage>(
            "endpoint",
            "test",
            Topology.DefaultName,
            typeof(SampleMessageHandler),
            typeof(PayloadCodecMessageDeserializer),
            "sample",
            MessageHandlerInvocation.Create<SampleMessage, SampleMessageHandler>()
        );
        var topology = new TestTopology(
            Topology.DefaultName,
            endpointsByName: new Dictionary<string, InboundEndpoint>(StringComparer.Ordinal)
            {
                [endpoint.Name] = endpoint
            }
        );

        var missingRequired = () => topology.GetRequiredEndpoint("missing");

        topology.GetRequiredEndpoint("endpoint").Should().BeSameAs(endpoint);
        topology.TryGetEndpoint("endpoint", out var resolved).Should().BeTrue();
        resolved.Should().BeSameAs(endpoint);
        topology.TryGetEndpoint("missing", out var missing).Should().BeFalse();
        missing.Should().BeNull();
        missingRequired.Should().Throw<InboundEndpointNotFoundException>()
           .Which.EndpointName.Should().Be("missing");
    }

    [Fact]
    public void EndpointLookups_RejectBlankNames()
    {
        var topology = new TestTopology(Topology.DefaultName);

        var required = () => topology.GetRequiredEndpoint(" ");
        var tryGet = () => topology.TryGetEndpoint(" ", out _);

        required.Should().Throw<ArgumentException>().WithParameterName("name");
        tryGet.Should().Throw<ArgumentException>().WithParameterName("name");
    }

    [Fact]
    public void InboundEndpointNotFoundException_PreservesEndpointNameAndInnerException()
    {
        InvalidOperationException inner = new ("inner");

        var exception = new InboundEndpointNotFoundException("endpoint", inner);

        exception.EndpointName.Should().Be("endpoint");
        exception.InnerException.Should().BeSameAs(inner);
        exception.Message.Should().Be("Inbound endpoint 'endpoint' is not registered.");
    }

    [Fact]
    public void TopologyData_PrepareTopologyDataStructures_RejectsNullDictionaries()
    {
        var targetsByMessageType = new Dictionary<Type, OutboundTarget>();
        var targetsByName = new Dictionary<string, OutboundTarget>(StringComparer.Ordinal);
        var endpointsByName = new Dictionary<string, InboundEndpoint>(StringComparer.Ordinal);

        var nullMessageTargets = () => TopologyData.PrepareTopologyDataStructures(
            null!,
            targetsByName,
            endpointsByName
        );
        var nullNamedTargets = () => TopologyData.PrepareTopologyDataStructures(
            targetsByMessageType,
            null!,
            endpointsByName
        );
        var nullEndpoints = () => TopologyData.PrepareTopologyDataStructures(
            targetsByMessageType,
            targetsByName,
            null!
        );

        nullMessageTargets.Should().Throw<ArgumentNullException>().WithParameterName("targetsByMessageType");
        nullNamedTargets.Should().Throw<ArgumentNullException>().WithParameterName("targetsByName");
        nullEndpoints.Should().Throw<ArgumentNullException>().WithParameterName("endpointsByName");
    }

    [Fact]
    public void SingleTopologyRegistry_ExposesDefaultTopologyAndReportsMissingNames()
    {
        var topology = EmptyTopology.Create();
        SingleTopologyRegistry registry = new (topology);

        registry.Names.Should().ContainSingle().Which.Should().Be(Topology.DefaultName);
        registry.TryGetTopology(Topology.DefaultName, out var resolved).Should().BeTrue();
        resolved.Should().BeSameAs(topology);
        registry.GetRequiredTopology(Topology.DefaultName).Should().BeSameAs(topology);
        registry.TryGetTopology("missing", out var missing).Should().BeFalse();
        missing.Should().BeNull();
        var getMissing = () => registry.GetRequiredTopology("missing");

        getMissing.Should().Throw<InvalidOperationException>()
           .WithMessage("Topology 'missing' is not registered. Registered topologies: default.");
    }

    [Fact]
    public void SingleTopologyRegistry_RejectsNullTopology()
    {
        var act = () => new SingleTopologyRegistry(null!);

        act.Should().Throw<ArgumentNullException>().WithParameterName("topology");
    }

    private sealed class SampleMessageHandler : IMessageHandler<SampleMessage>
    {
        public Task HandleAsync(
            SampleMessage message,
            IncomingMessageContext context,
            CancellationToken cancellationToken
        )
        {
            return Task.CompletedTask;
        }
    }
}
