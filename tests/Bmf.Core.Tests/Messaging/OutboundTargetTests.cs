using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Abstractions;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Outbound;
using BrilliantMessaging.Core.Tests.Messaging.TestSupport;
using FluentAssertions;
using Xunit;

namespace BrilliantMessaging.Core.Tests.Messaging;

// Direct target publishes now emit outbound publish metrics, so this class joins the serialized
// Diagnostics collection to avoid contaminating listener-based diagnostics tests.
[Collection("Diagnostics")]
public sealed class OutboundTargetTests
{
    [Fact]
    public async Task PublishAsync_ForwardsRoutingKeyToTypedDispatch()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var target = new RecordingTarget<SampleMessage>(
            "default",
            CloudEventsTestFactory.CreateSerializer()
        );

        await target.PublishAsync(
            new SampleMessage("hello"),
            "target.route",
            cancellationToken
        );

        target.RoutingKeys.Should().ContainSingle().Which.Should().Be("target.route");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task RoutablePublishAsync_RejectsBlankRoutingKey(string? routingKey)
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        IOutboundRoutableTarget<SampleMessage> target = new RecordingTarget<SampleMessage>(
            "default",
            CloudEventsTestFactory.CreateSerializer()
        );

        var act = async () => await target.PublishAsync(
            new SampleMessage("hello"),
            routingKey!,
            cancellationToken
        );

        await act.Should().ThrowAsync<ArgumentException>().WithParameterName("routingKey");
    }

    [Fact]
    public async Task PublishAsync_WrapsSerializationFailures()
    {
        var serializer = new ThrowingSerializer(new InvalidOperationException("boom"));
        var target = new RecordingTarget<SampleMessage>("default", serializer);

        var act = async () => await target.PublishAsync(new SampleMessage("hello"));

        var exception = (await act.Should().ThrowAsync<MessageSerializationException>()).Which;
        exception.InnerException.Should().BeOfType<InvalidOperationException>();
        exception.MessageType.Should().Be<SampleMessage>();
    }

    [Fact]
    public async Task PublishAsync_DoesNotWrapCancellation()
    {
        var serializer = new ThrowingSerializer(new OperationCanceledException());
        var target = new RecordingTarget<SampleMessage>("default", serializer);

        var act = async () => await target.PublishAsync(new SampleMessage("hello"));

        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task PublishAsync_ReportsMissingCloudEventIdWhenMessageDoesNotProvideMetadata()
    {
        var target = new RecordingTarget<ThirdPartyMessage>(
            "third-party",
            CloudEventsTestFactory.CreateSerializer(),
            CloudEventsTestFactory.CreateRegistry(
                new KeyValuePair<Type, string>(typeof(ThirdPartyMessage), "tests.third-party")
            )
        );

        var act = async () => await target.PublishAsync(new ThirdPartyMessage("hello"));

        var exception = (await act.Should().ThrowAsync<CloudEventMetadataException>()).Which;
        exception.AttributeName.Should().Be(CloudEventAttributeNames.Id);
    }

    [Theory]
    [InlineData("name")]
    [InlineData("transportName")]
    [InlineData("topologyName")]
    public void Constructor_RejectsBlankTextArguments(string parameterName)
    {
        var name = parameterName == "name" ? " " : "target";
        var transportName = parameterName == "transportName" ? " " : "test";
        var topologyName = parameterName == "topologyName" ? " " : Topology.DefaultName;

        var act = () => new TestTarget(name, transportName, topologyName);

        act.Should().Throw<ArgumentException>().WithParameterName(parameterName);
    }

    [Fact]
    public void GenericConstructor_RejectsNullSerializerAndRegistry()
    {
        var registry = CloudEventsTestFactory.CreateRegistry();
        var serializer = CloudEventsTestFactory.CreateSerializer();

        var nullSerializer = () => new RecordingTarget<SampleMessage>("target", null!, registry);
        var nullRegistry = () => new RecordingTarget<SampleMessage>("target", serializer, null!);

        nullSerializer.Should().Throw<ArgumentNullException>().WithParameterName("serializer");
        nullRegistry.Should().Throw<ArgumentNullException>().WithParameterName("messageContractRegistry");
    }

    [Fact]
    public async Task PublishAsync_RejectsNullMessage()
    {
        var target = new RecordingTarget<SampleMessage>("target", CloudEventsTestFactory.CreateSerializer());
        CloudEventMetadata metadata = new (Guid.NewGuid(), DateTimeOffset.UtcNow);

        var act = async () => await target.PublishAsync(null!, in metadata);

        await act.Should().ThrowAsync<ArgumentNullException>().WithParameterName("message");
    }

    [Fact]
    public void ContractLookup_RejectsNullRuntimeMessageType()
    {
        var target = new RecordingTarget<SampleMessage>("target", CloudEventsTestFactory.CreateSerializer());

        var discriminator = () => target.GetRequiredDiscriminator(null!);
        var dataSchema = () => target.GetDataSchema(null!);

        discriminator.Should().Throw<ArgumentNullException>().WithParameterName("runtimeMessageType");
        dataSchema.Should().Throw<ArgumentNullException>().WithParameterName("runtimeMessageType");
    }

    private sealed class TestTarget : OutboundTarget
    {
        public TestTarget(string name, string transportName, string? topologyName)
            : base(name, transportName, topologyName) { }

        protected override Task PublishSerializedCoreAsync(
            SerializedMessage message,
            CancellationToken cancellationToken
        )
        {
            return Task.CompletedTask;
        }
    }
}
