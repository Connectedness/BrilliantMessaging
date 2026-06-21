using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Bmf.Core.Messaging;
using Bmf.Core.Messaging.Outbound;
using Bmf.Core.Tests.Messaging.TestSupport;
using FluentAssertions;
using Xunit;

namespace Bmf.Core.Tests.Messaging;

/// <summary>
/// Verifies that publish diagnostics are owned by the target layer and labelled with the OpenTelemetry
/// <c>messaging.*</c> semantic conventions, so direct target publishes and publisher-mediated publishes emit
/// identical activities and metrics without nesting or double counting.
/// </summary>
[Collection("Diagnostics")]
public sealed class OutboundTargetDiagnosticsTests
{
    private const string PublishActivityName = "bmf.outbound.publish";

    private const string Destination = "recording-exchange";

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
        activity.DisplayName.Should().Be($"{MessagingSemanticConventions.SendOperation} {Destination}");
        activity.GetTagItem(MessagingSemanticConventions.MessagingSystem).Should().Be("test");
        activity.GetTagItem(MessagingSemanticConventions.MessagingOperationType)
           .Should().Be(MessagingSemanticConventions.SendOperation);
        activity.GetTagItem(MessagingSemanticConventions.MessagingOperationName)
           .Should().Be(MessagingSemanticConventions.SendOperation);
        activity.GetTagItem(MessagingSemanticConventions.MessagingDestinationName).Should().Be(Destination);
        activity.GetTagItem(MessagingSemanticConventions.MessagingMessageId).Should().NotBeNull();
        activity.GetTagItem(MessagingSemanticConventions.MessagingMessageBodySize).Should().BeOfType<int>();
        activity.Status.Should().Be(ActivityStatusCode.Ok);
        activity.GetTagItem(MessagingSemanticConventions.ErrorType).Should().BeNull();

        var sent = recorder.SentMessages.Should().ContainSingle().Which;
        sent.Should().Contain(
            new KeyValuePair<string, object?>(MessagingSemanticConventions.MessagingSystem, "test")
        );
        sent.Should().Contain(
            new KeyValuePair<string, object?>(MessagingSemanticConventions.MessagingDestinationName, Destination)
        );
        sent.Should().NotContain(tag => tag.Key == MessagingSemanticConventions.ErrorType);
        recorder.Durations.Should().ContainSingle();
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
        activity.GetTagItem(MessagingSemanticConventions.MessagingSystem).Should().Be("test");
        activity.GetTagItem(MessagingSemanticConventions.MessagingDestinationName).Should().Be(Destination);
        activity.GetTagItem(MessagingSemanticConventions.MessagingMessageBodySize).Should().Be(message.Body.Length);
        activity.Status.Should().Be(ActivityStatusCode.Ok);

        recorder.SentMessages.Should().ContainSingle();
        recorder.Durations.Should().ContainSingle();
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
           .Which.Status.Should().Be(ActivityStatusCode.Ok);
        recorder.SentMessages.Should().ContainSingle();
        recorder.Durations.Should().ContainSingle();
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
           .Which.Status.Should().Be(ActivityStatusCode.Ok);
        recorder.SentMessages.Should().ContainSingle();
        recorder.Durations.Should().ContainSingle();
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
        activity.GetTagItem(MessagingSemanticConventions.MessagingOperationName)
           .Should().Be(MessagingSemanticConventions.SendOperation);
        activity.Status.Should().Be(ActivityStatusCode.Ok);

        recorder.SentMessages.Should().ContainSingle();
        recorder.Durations.Should().ContainSingle();
        // The RabbitMQ-namespaced routing-key tag is set by the transport, not the Core funnel; the Core target
        // only routes the key to the transport, which the recording double captures here.
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
           .Which.Status.Should().Be(ActivityStatusCode.Ok);
        recorder.SentMessages.Should().ContainSingle();
        recorder.Durations.Should().ContainSingle();
        target.RoutingKeys.Should().ContainSingle().Which.Should().Be("orders.created");
    }

    [Fact]
    public async Task Publish_WhenCallerTokenCancelled_RecordsCancellationWithoutErrorType()
    {
        using var recorder = new OutboundDiagnosticsRecorder();
        using CancellationTokenSource cancellationTokenSource = new ();
        await cancellationTokenSource.CancelAsync();
        var target = new RecordingTarget<SampleMessage>(
            "default",
            new ThrowingSerializer(new OperationCanceledException())
        );

        // ReSharper disable once AccessToDisposedClosure -- act is awaited before disposal
        var act = async () => await target.PublishAsync(new SampleMessage("hello"), cancellationTokenSource.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
        // A cancellation of the caller's token is an ordinary counter increment with error.type absent, like success.
        recorder.SentMessages.Should().ContainSingle().Which.Should()
           .NotContain(tag => tag.Key == MessagingSemanticConventions.ErrorType);
        recorder.Durations.Should().ContainSingle().Which.Should()
           .NotContain(tag => tag.Key == MessagingSemanticConventions.ErrorType);
        recorder
           .StartedActivities.Should().ContainSingle()
           .Which.GetTagItem(MessagingSemanticConventions.ErrorType).Should().BeNull();
    }

    [Fact]
    public async Task Publish_WhenCancellationIsNotFromCallerToken_RecordsFailureWithOtherErrorType()
    {
        using var recorder = new OutboundDiagnosticsRecorder();
        // The caller's token is never signalled, so an OperationCanceledException from inside the publish is an
        // unexpected cancellation (e.g. an unrelated internal timeout) and must be classified as a failure rather
        // than suppressed as a graceful cancellation.
        var target = new RecordingTarget<SampleMessage>(
            "default",
            new ThrowingSerializer(new OperationCanceledException())
        );

        var act = async () => await target.PublishAsync(new SampleMessage("hello"));

        await act.Should().ThrowAsync<OperationCanceledException>();
        recorder.SentMessages.Should().ContainSingle().Which.Should().Contain(
            new KeyValuePair<string, object?>(
                MessagingSemanticConventions.ErrorType,
                MessagingSemanticConventions.ErrorTypeOther
            )
        );
        recorder.Durations.Should().ContainSingle().Which.Should().Contain(
            new KeyValuePair<string, object?>(
                MessagingSemanticConventions.ErrorType,
                MessagingSemanticConventions.ErrorTypeOther
            )
        );
        var activity = recorder.StartedActivities.Should().ContainSingle().Which;
        activity.Status.Should().Be(ActivityStatusCode.Error);
        activity.GetTagItem(MessagingSemanticConventions.ErrorType)
           .Should().Be(MessagingSemanticConventions.ErrorTypeOther);
    }

    [Fact]
    public async Task Publish_RecordsSerializationFailureWithOtherErrorType()
    {
        using var recorder = new OutboundDiagnosticsRecorder();
        var target = new RecordingTarget<SampleMessage>(
            "default",
            new ThrowingSerializer(new InvalidOperationException("boom"))
        );

        var act = async () => await target.PublishAsync(new SampleMessage("hello"));

        await act.Should().ThrowAsync<MessageSerializationException>();
        recorder.SentMessages.Should().ContainSingle().Which.Should().Contain(
            new KeyValuePair<string, object?>(
                MessagingSemanticConventions.ErrorType,
                MessagingSemanticConventions.ErrorTypeOther
            )
        );
        recorder.Durations.Should().ContainSingle().Which.Should().Contain(
            new KeyValuePair<string, object?>(
                MessagingSemanticConventions.ErrorType,
                MessagingSemanticConventions.ErrorTypeOther
            )
        );
        var activity = recorder.StartedActivities.Should().ContainSingle().Which;
        activity.Status.Should().Be(ActivityStatusCode.Error);
        activity.GetTagItem(MessagingSemanticConventions.ErrorType)
           .Should().Be(MessagingSemanticConventions.ErrorTypeOther);
        activity.Events.Should().ContainSingle(@event => @event.Name == "exception");
    }

    [Fact]
    public async Task Publish_RecordsDeliveryFailureReasonAsBoundedErrorType()
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

        var act = async () => await target.PublishAsync(new SampleMessage("hello"));

        await act.Should().ThrowAsync<MessageDeliveryException>();
        recorder.SentMessages.Should().ContainSingle().Which.Should().Contain(
            new KeyValuePair<string, object?>(MessagingSemanticConventions.ErrorType, "nacked")
        );
        var activity = recorder.StartedActivities.Should().ContainSingle().Which;
        activity.GetTagItem(MessagingSemanticConventions.ErrorType).Should().Be("nacked");
        activity.Status.Should().Be(ActivityStatusCode.Error);
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
