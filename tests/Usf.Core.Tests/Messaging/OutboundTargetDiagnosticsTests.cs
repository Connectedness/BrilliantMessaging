using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using FluentAssertions;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Errors;
using Usf.Core.Tests.Messaging.TestSupport;
using Xunit;

namespace Usf.Core.Tests.Messaging;

/// <summary>
/// Verifies that publish diagnostics are owned by the target layer, so direct target publishes and
/// publisher-mediated publishes emit identical activities and metrics without nesting or double counting.
/// </summary>
[Collection("Diagnostics")]
public sealed class OutboundTargetDiagnosticsTests
{
    private const string PublishActivityName = "usf.outbound.publish";

    [Fact]
    public async Task DirectTypedPublish_RecordsSingleInstrumentedPublish()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        using var recorder = new OutboundDiagnosticsRecorder();
        var target = new RecordingTarget<SampleMessage>("default", CloudEventsTestFactory.CreateSerializer());

        await target.PublishAsync(new SampleMessage("hello"), cancellationToken);

        var activity = recorder.StartedActivities.Should().ContainSingle().Which;
        activity.OperationName.Should().Be(PublishActivityName);
        activity.Kind.Should().Be(ActivityKind.Producer);
        activity
           .GetTagItem(OutboundDiagnostics.MessageTypeTagName)
           .Should().Be(CloudEventsTestFactory.SampleDiscriminator);
        activity.GetTagItem(OutboundDiagnostics.TargetNameTagName).Should().Be("default");
        activity.GetTagItem(OutboundDiagnostics.TransportNameTagName).Should().Be("test");
        activity.GetTagItem(OutboundDiagnostics.OutcomeTagName).Should().Be("success");

        recorder.Attempts.Should().ContainSingle().Which.Should().Contain(
            new KeyValuePair<string, object?>(
                OutboundDiagnostics.MessageTypeTagName,
                CloudEventsTestFactory.SampleDiscriminator
            )
        );
        recorder.Durations.Should().ContainSingle();
        recorder.Failures.Should().BeEmpty();
    }

    [Fact]
    public async Task DirectRawPublish_RecordsSingleInstrumentedPublish()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        using var recorder = new OutboundDiagnosticsRecorder();
        var target = new RecordingTarget<SampleMessage>("raw", CloudEventsTestFactory.CreateSerializer());
        var message = CreateSerializedMessage();

        await target.PublishSerializedAsync(message, cancellationToken);

        var activity = recorder.StartedActivities.Should().ContainSingle().Which;
        activity.OperationName.Should().Be(PublishActivityName);
        // The raw path derives the message-type tag from the target rather than the typed discriminator.
        activity
           .GetTagItem(OutboundDiagnostics.MessageTypeTagName)
           .Should().Be(CloudEventsTestFactory.SampleDiscriminator);
        activity.GetTagItem(OutboundDiagnostics.TargetNameTagName).Should().Be("raw");
        activity.GetTagItem(OutboundDiagnostics.OutcomeTagName).Should().Be("success");

        recorder.Attempts.Should().ContainSingle();
        recorder.Durations.Should().ContainSingle();
        recorder.Failures.Should().BeEmpty();
        target.SerializedMessages.Should().ContainSingle().Which.Should().Be(message);
    }

    [Fact]
    public async Task PublisherMediatedTypedPublish_RecordsSinglePublishWithoutNestingOrDoubleCounting()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        using var recorder = new OutboundDiagnosticsRecorder();
        var target = new RecordingTarget<SampleMessage>("default", CloudEventsTestFactory.CreateSerializer());
        var publisher = new MessagePublisher(EmptyTopology.Create());

        await publisher.PublishMessageAsync(new SampleMessage("hello"), target, cancellationToken: cancellationToken);

        recorder
           .StartedActivities.Should().ContainSingle()
           .Which.GetTagItem(OutboundDiagnostics.OutcomeTagName).Should().Be("success");
        recorder.Attempts.Should().ContainSingle();
        recorder.Durations.Should().ContainSingle();
        recorder.Failures.Should().BeEmpty();
    }

    [Fact]
    public async Task PublisherMediatedRawPublish_RecordsSinglePublishWithoutNestingOrDoubleCounting()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        using var recorder = new OutboundDiagnosticsRecorder();
        var target = new RecordingTarget<SampleMessage>("raw", CloudEventsTestFactory.CreateSerializer());
        var publisher = new MessagePublisher(EmptyTopology.Create());

        await publisher.PublishRawAsync(CreateSerializedMessage(), target, cancellationToken);

        recorder
           .StartedActivities.Should().ContainSingle()
           .Which.GetTagItem(OutboundDiagnostics.OutcomeTagName).Should().Be("success");
        recorder.Attempts.Should().ContainSingle();
        recorder.Durations.Should().ContainSingle();
        recorder.Failures.Should().BeEmpty();
    }

    [Fact]
    public async Task DirectRoutingKeyPublish_RecordsSingleInstrumentedPublish()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        using var recorder = new OutboundDiagnosticsRecorder();
        var target = new RecordingTarget<SampleMessage>("default", CloudEventsTestFactory.CreateSerializer());

        await target.PublishAsync(new SampleMessage("hello"), "orders.created", cancellationToken);

        var activity = recorder.StartedActivities.Should().ContainSingle().Which;
        activity.OperationName.Should().Be(PublishActivityName);
        activity
           .GetTagItem(OutboundDiagnostics.MessageTypeTagName)
           .Should().Be(CloudEventsTestFactory.SampleDiscriminator);
        activity.GetTagItem(OutboundDiagnostics.OutcomeTagName).Should().Be("success");

        recorder.Attempts.Should().ContainSingle();
        recorder.Durations.Should().ContainSingle();
        recorder.Failures.Should().BeEmpty();
        target.RoutingKeys.Should().ContainSingle().Which.Should().Be("orders.created");
    }

    [Fact]
    public async Task PublisherMediatedRoutingKeyPublish_RecordsSinglePublishWithoutNestingOrDoubleCounting()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        using var recorder = new OutboundDiagnosticsRecorder();
        var target = new RecordingTarget<SampleMessage>("default", CloudEventsTestFactory.CreateSerializer());
        var publisher = new MessagePublisher(EmptyTopology.Create());

        await publisher.PublishMessageAsync(
            new SampleMessage("hello"),
            target,
            "orders.created",
            cancellationToken
        );

        recorder
           .StartedActivities.Should().ContainSingle()
           .Which.GetTagItem(OutboundDiagnostics.OutcomeTagName).Should().Be("success");
        recorder.Attempts.Should().ContainSingle();
        recorder.Durations.Should().ContainSingle();
        recorder.Failures.Should().BeEmpty();
        target.RoutingKeys.Should().ContainSingle().Which.Should().Be("orders.created");
    }

    [Fact]
    public async Task Publish_RecordsCancellationOutcomeWithoutFailureCount()
    {
        using var recorder = new OutboundDiagnosticsRecorder();
        var target = new RecordingTarget<SampleMessage>(
            "default",
            new ThrowingSerializer(new OperationCanceledException())
        );

        var action = async () => await target.PublishAsync(new SampleMessage("hello"));

        await action.Should().ThrowAsync<OperationCanceledException>();
        recorder.Attempts.Should().ContainSingle();
        recorder.Failures.Should().BeEmpty();
        recorder.Durations.Should().ContainSingle().Which.Should().Contain(
            new KeyValuePair<string, object?>(OutboundDiagnostics.OutcomeTagName, "cancelled")
        );
        recorder
           .StartedActivities.Should().ContainSingle()
           .Which.GetTagItem(OutboundDiagnostics.OutcomeTagName).Should().Be("cancelled");
    }

    [Fact]
    public async Task Publish_RecordsSerializationFailureWithoutDeliveryReason()
    {
        using var recorder = new OutboundDiagnosticsRecorder();
        var target = new RecordingTarget<SampleMessage>(
            "default",
            new ThrowingSerializer(new InvalidOperationException("boom"))
        );

        var action = async () => await target.PublishAsync(new SampleMessage("hello"));

        await action.Should().ThrowAsync<MessageSerializationException>();
        var failure = recorder.Failures.Should().ContainSingle().Which;
        failure.Should().Contain(
            new KeyValuePair<string, object?>(OutboundDiagnostics.OutcomeTagName, "failure")
        );
        failure.Should().NotContain(tag => tag.Key == OutboundDiagnostics.DeliveryFailureReasonTagName);
        recorder
           .StartedActivities.Should().ContainSingle()
           .Which.GetTagItem(OutboundDiagnostics.OutcomeTagName).Should().Be("failure");
    }

    [Fact]
    public async Task Publish_RecordsDeliveryFailureReason()
    {
        using var recorder = new OutboundDiagnosticsRecorder();
        var deliveryException = new MessageDeliveryException(
            "default",
            MessageDeliveryFailureReason.Nacked,
            new InvalidOperationException("nacked")
        );
        var target = new ThrowingTarget<SampleMessage>(
            "default",
            CloudEventsTestFactory.CreateSerializer(),
            deliveryException
        );

        var action = async () => await target.PublishAsync(new SampleMessage("hello"));

        await action.Should().ThrowAsync<MessageDeliveryException>();
        var failure = recorder.Failures.Should().ContainSingle().Which;
        failure.Should().Contain(
            new KeyValuePair<string, object?>(OutboundDiagnostics.OutcomeTagName, "failure")
        );
        failure.Should().Contain(
            new KeyValuePair<string, object?>(OutboundDiagnostics.DeliveryFailureReasonTagName, "nacked")
        );
        recorder
           .StartedActivities.Should().ContainSingle()
           .Which.GetTagItem(OutboundDiagnostics.DeliveryFailureReasonTagName).Should().Be("nacked");
    }

    private static SerializedMessage CreateSerializedMessage()
    {
        return new SerializedMessage(
            "prepared"u8.ToArray(),
            "application/custom",
            "utf-8",
            new Dictionary<string, string?>(StringComparer.Ordinal),
            null,
            null
        );
    }
}
