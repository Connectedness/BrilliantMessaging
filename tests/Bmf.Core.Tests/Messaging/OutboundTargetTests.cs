using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FluentAssertions;
using Bmf.Abstractions;
using Bmf.Core.Messaging.Outbound;
using Bmf.Core.Tests.Messaging.TestSupport;
using Xunit;

namespace Bmf.Core.Tests.Messaging;

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
}
