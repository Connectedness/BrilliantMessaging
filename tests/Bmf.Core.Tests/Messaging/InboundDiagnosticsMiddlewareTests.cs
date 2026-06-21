using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Bmf.Core.Messaging;
using Bmf.Core.Messaging.Inbound;
using Bmf.Core.Tests.Messaging.TestSupport;
using FluentAssertions;
using Xunit;

namespace Bmf.Core.Tests.Messaging;

[Collection("Diagnostics")]
public sealed class InboundDiagnosticsMiddlewareTests
{
    private const string ProcessActivityName = "bmf.inbound.process";

    [Fact]
    public async Task InvokeAsync_ParentsConsumerActivityToExtractedTraceContext()
    {
        using var producer = new Activity("producer").SetIdFormat(ActivityIdFormat.W3C);
        producer.TraceStateString = "vendor=value";
        producer.AddBaggage("tenant", "tenant-7").Start();
        var headers = new Dictionary<string, object?>();
        TraceContextHeaders.Inject(headers, producer);
        producer.Stop();

        using var recorder = new InboundDiagnosticsRecorder();
        var context = CreateContext(headers, TestContext.Current.CancellationToken);
        var middleware = new InboundDiagnosticsMiddleware();
        Activity? currentActivity = null;
        string? baggageValue = null;

        await middleware.InvokeAsync(
            context,
            incomingContext =>
            {
                currentActivity = Activity.Current;
                baggageValue = Activity.Current?.Baggage.Single(pair => pair.Key == "tenant").Value;
                incomingContext.Endpoint.Discriminator.Should().Be("tests.message");
                return Task.CompletedTask;
            }
        );

        var activity = recorder.StartedActivities.Should().ContainSingle().Which;
        activity.Should().BeSameAs(currentActivity);
        activity.OperationName.Should().Be(ProcessActivityName);
        activity.Kind.Should().Be(ActivityKind.Consumer);
        activity.TraceId.Should().Be(producer.TraceId);
        activity.ParentSpanId.Should().Be(producer.SpanId);
        activity.TraceStateString.Should().Be("vendor=value");
        baggageValue.Should().Be("tenant-7");
        activity.GetTagItem(InboundDiagnostics.MessageTypeTagName).Should().Be("tests.message");
        activity.GetTagItem(InboundDiagnostics.EndpointNameTagName).Should().Be("endpoint");
        activity.GetTagItem(InboundDiagnostics.SourceTagName).Should().Be("source");
        activity.GetTagItem(InboundDiagnostics.TransportNameTagName).Should().Be("test");
        activity.GetTagItem(InboundDiagnostics.OutcomeTagName).Should().Be("success");
        activity.Status.Should().Be(ActivityStatusCode.Ok);

        var attempt = recorder.Attempts.Should().ContainSingle().Which;
        attempt.Should().Contain(
            new KeyValuePair<string, object?>(InboundDiagnostics.MessageTypeTagName, "tests.message"),
            new KeyValuePair<string, object?>(InboundDiagnostics.EndpointNameTagName, "endpoint"),
            new KeyValuePair<string, object?>(InboundDiagnostics.SourceTagName, "source"),
            new KeyValuePair<string, object?>(InboundDiagnostics.TransportNameTagName, "test")
        );
        attempt.Should().NotContain(tag => tag.Key == InboundDiagnostics.OutcomeTagName);
        recorder.Durations.Should().ContainSingle().Which.Should().Contain(
            new KeyValuePair<string, object?>(InboundDiagnostics.MessageTypeTagName, "tests.message")
        );
        recorder.Failures.Should().BeEmpty();
    }

    [Fact]
    public async Task InvokeAsync_StartsRootConsumerActivityWhenNoTraceParentIsPresent()
    {
        using var recorder = new InboundDiagnosticsRecorder();
        var middleware = new InboundDiagnosticsMiddleware();

        await middleware.InvokeAsync(
            CreateContext(cancellationToken: TestContext.Current.CancellationToken),
            static _ => Task.CompletedTask
        );

        var activity = recorder.StartedActivities.Should().ContainSingle().Which;
        activity.ParentSpanId.ToString().Should().Be("0000000000000000");
        activity.ParentId.Should().BeNull();
        activity.GetTagItem(InboundDiagnostics.OutcomeTagName).Should().Be("success");
        recorder.Attempts.Should().ContainSingle();
        recorder.Durations.Should().ContainSingle();
    }

    [Fact]
    public async Task InvokeAsync_RecordsMetricsOnlyWhenNoListenerIsRegistered()
    {
        using var recorder = new InboundDiagnosticsRecorder(captureActivities: false);
        var middleware = new InboundDiagnosticsMiddleware();
        Activity? currentActivity = null;

        await middleware.InvokeAsync(
            CreateContext(cancellationToken: TestContext.Current.CancellationToken),
            _ =>
            {
                currentActivity = Activity.Current;
                return Task.CompletedTask;
            }
        );

        currentActivity.Should().BeNull();
        recorder.StartedActivities.Should().BeEmpty();
        recorder.Attempts.Should().ContainSingle();
        recorder.Durations.Should().ContainSingle();
    }

    [Fact]
    public async Task InvokeAsync_RecordsAttemptBeforeCallingNextWithoutOutcome()
    {
        using var recorder = new InboundDiagnosticsRecorder();
        var middleware = new InboundDiagnosticsMiddleware();

        await middleware.InvokeAsync(
            CreateContext(cancellationToken: TestContext.Current.CancellationToken),
            _ =>
            {
                recorder
                   .Attempts.Should().ContainSingle()
                   .Which.Should().NotContain(tag => tag.Key == InboundDiagnostics.OutcomeTagName);
                recorder.Failures.Should().BeEmpty();
                recorder.Durations.Should().BeEmpty();
                return Task.CompletedTask;
            }
        );

        recorder.Durations.Should().ContainSingle().Which.Should().Contain(
            new KeyValuePair<string, object?>(InboundDiagnostics.OutcomeTagName, "success")
        );
    }

    [Fact]
    public async Task InvokeAsync_RecordsFailureOutcomeAndException()
    {
        using var recorder = new InboundDiagnosticsRecorder();
        var middleware = new InboundDiagnosticsMiddleware();
        var exception = new InvalidOperationException("boom");

        var act = async () => await middleware.InvokeAsync(
            CreateContext(cancellationToken: TestContext.Current.CancellationToken),
            _ => throw exception
        );

        await act.Should().ThrowAsync<InvalidOperationException>();
        recorder
           .Attempts.Should().ContainSingle()
           .Which.Should().NotContain(tag => tag.Key == InboundDiagnostics.OutcomeTagName);
        recorder.Failures.Should().ContainSingle().Which.Should().Contain(
            new KeyValuePair<string, object?>(InboundDiagnostics.OutcomeTagName, "failure")
        );
        recorder.Durations.Should().ContainSingle().Which.Should().Contain(
            new KeyValuePair<string, object?>(InboundDiagnostics.OutcomeTagName, "failure")
        );

        var activity = recorder.StartedActivities.Should().ContainSingle().Which;
        activity.Status.Should().Be(ActivityStatusCode.Error);
        activity.GetTagItem(InboundDiagnostics.OutcomeTagName).Should().Be("failure");
        activity.Events.Should().ContainSingle(@event => @event.Name == "exception");
    }

    [Fact]
    public async Task InvokeAsync_RecordsCancellationOutcomeWithoutFailureCount()
    {
        using var recorder = new InboundDiagnosticsRecorder();
        using CancellationTokenSource cancellationTokenSource = new ();
        await cancellationTokenSource.CancelAsync();
        var middleware = new InboundDiagnosticsMiddleware();

        var act = async () => await middleware.InvokeAsync(
            // ReSharper disable AccessToDisposedClosure -- act is called before disposal
            CreateContext(cancellationToken: cancellationTokenSource.Token),
            _ => throw new OperationCanceledException(cancellationTokenSource.Token)
            // ReSharper restore AccessToDisposedClosure
        );

        await act.Should().ThrowAsync<OperationCanceledException>();
        recorder
           .Attempts.Should().ContainSingle()
           .Which.Should().NotContain(tag => tag.Key == InboundDiagnostics.OutcomeTagName);
        recorder.Failures.Should().BeEmpty();
        recorder.Durations.Should().ContainSingle().Which.Should().Contain(
            new KeyValuePair<string, object?>(InboundDiagnostics.OutcomeTagName, "cancelled")
        );
        recorder
           .StartedActivities.Should().ContainSingle()
           .Which.GetTagItem(InboundDiagnostics.OutcomeTagName).Should().Be("cancelled");
    }

    private static IncomingMessageContext CreateContext(
        IReadOnlyDictionary<string, object?>? headers = null,
        CancellationToken cancellationToken = default
    )
    {
        return new IncomingMessageContext(
            new TestTransportMessage(headers),
            new InboundEndpoint<TestMessage>(
                "endpoint",
                "test",
                Topology.DefaultName,
                typeof(TestHandler),
                typeof(PayloadCodecMessageDeserializer),
                "tests.message",
                MessageHandlerInvocation.Create<TestMessage, TestHandler>()
            ),
            EmptyServiceProvider.Instance,
            NoOpAcknowledgement.Instance,
            cancellationToken,
            typeof(TestMessage)
        );
    }

    private sealed record TestMessage;

    private sealed class TestHandler : IMessageHandler<TestMessage>
    {
        public Task HandleAsync(
            TestMessage message,
            IncomingMessageContext context,
            CancellationToken cancellationToken
        )
        {
            return Task.CompletedTask;
        }
    }

    private sealed class TestTransportMessage : TransportMessage
    {
        public TestTransportMessage(IReadOnlyDictionary<string, object?>? headers)
            : base(
                "test",
                "source",
                ReadOnlyMemory<byte>.Empty,
                headers ?? new Dictionary<string, object?>()
            ) { }
    }

    private sealed class NoOpAcknowledgement : IMessageAcknowledgement
    {
        private NoOpAcknowledgement() { }

        public static NoOpAcknowledgement Instance { get; } = new ();

        public Task AckAsync(CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }

        public Task NackAsync(bool requeue, CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }
    }

    private sealed class EmptyServiceProvider : IServiceProvider
    {
        private EmptyServiceProvider() { }

        public static EmptyServiceProvider Instance { get; } = new ();

        public object? GetService(Type serviceType)
        {
            return null;
        }
    }
}
