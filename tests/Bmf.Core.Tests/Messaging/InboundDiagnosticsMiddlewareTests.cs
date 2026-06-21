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
        activity.DisplayName.Should().Be($"{MessagingSemanticConventions.ProcessOperation} source");
        activity.TraceId.Should().Be(producer.TraceId);
        activity.ParentSpanId.Should().Be(producer.SpanId);
        activity.TraceStateString.Should().Be("vendor=value");
        baggageValue.Should().Be("tenant-7");
        activity.GetTagItem(MessagingSemanticConventions.MessagingSystem).Should().Be("test");
        activity
           .GetTagItem(MessagingSemanticConventions.MessagingOperationType)
           .Should().Be(MessagingSemanticConventions.ProcessOperation);
        activity
           .GetTagItem(MessagingSemanticConventions.MessagingOperationName)
           .Should().Be(MessagingSemanticConventions.ProcessOperation);
        activity.GetTagItem(MessagingSemanticConventions.MessagingDestinationName).Should().Be("source");
        activity.GetTagItem(MessagingSemanticConventions.MessagingMessageBodySize).Should().Be(0);
        activity.Status.Should().Be(ActivityStatusCode.Ok);
        activity.GetTagItem(MessagingSemanticConventions.ErrorType).Should().BeNull();

        var consumed = recorder.ConsumedMessages.Should().ContainSingle().Which;
        consumed.Should().Contain(
            new KeyValuePair<string, object?>(MessagingSemanticConventions.MessagingSystem, "test"),
            new KeyValuePair<string, object?>(
                MessagingSemanticConventions.MessagingOperationName,
                MessagingSemanticConventions.ProcessOperation
            ),
            new KeyValuePair<string, object?>(MessagingSemanticConventions.MessagingDestinationName, "source")
        );
        consumed.Should().NotContain(tag => tag.Key == MessagingSemanticConventions.ErrorType);
        recorder.Durations.Should().ContainSingle().Which.Should().Contain(
            new KeyValuePair<string, object?>(MessagingSemanticConventions.MessagingDestinationName, "source")
        );
    }

    [Fact]
    public async Task InvokeAsync_SetsMessageIdAndRoutingKeyAttributesWhenPresent()
    {
        using var recorder = new InboundDiagnosticsRecorder();
        var middleware = new InboundDiagnosticsMiddleware();
        var context = new IncomingMessageContext(
            new TestTransportMessage(headers: null, messageId: "message-42", routingKey: "orders.created"),
            CreateEndpoint(),
            EmptyServiceProvider.Instance,
            NoOpAcknowledgement.Instance,
            TestContext.Current.CancellationToken,
            typeof(TestMessage)
        );

        await middleware.InvokeAsync(context, static _ => Task.CompletedTask);

        var activity = recorder.StartedActivities.Should().ContainSingle().Which;
        activity.GetTagItem(MessagingSemanticConventions.MessagingMessageId).Should().Be("message-42");
        activity
           .GetTagItem(MessagingSemanticConventions.MessagingRabbitMqDestinationRoutingKey)
           .Should().Be("orders.created");
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
        activity.GetTagItem(MessagingSemanticConventions.ErrorType).Should().BeNull();
        recorder.ConsumedMessages.Should().ContainSingle();
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
        recorder.ConsumedMessages.Should().ContainSingle();
        recorder.Durations.Should().ContainSingle();
    }

    [Fact]
    public async Task InvokeAsync_RecordsConsumedMessageAndDurationAfterNext()
    {
        using var recorder = new InboundDiagnosticsRecorder();
        var middleware = new InboundDiagnosticsMiddleware();

        await middleware.InvokeAsync(
            CreateContext(cancellationToken: TestContext.Current.CancellationToken),
            _ =>
            {
                // The headline counter and duration are recorded once at completion, not before the pipeline runs.
                recorder.ConsumedMessages.Should().BeEmpty();
                recorder.Durations.Should().BeEmpty();
                return Task.CompletedTask;
            }
        );

        recorder
           .ConsumedMessages.Should().ContainSingle().Which
           .Should().NotContain(tag => tag.Key == MessagingSemanticConventions.ErrorType);
        recorder
           .Durations.Should().ContainSingle().Which
           .Should().NotContain(tag => tag.Key == MessagingSemanticConventions.ErrorType);
    }

    [Fact]
    public async Task InvokeAsync_RecordsFailureErrorTypeAndException()
    {
        using var recorder = new InboundDiagnosticsRecorder();
        var middleware = new InboundDiagnosticsMiddleware();
        var exception = new InvalidOperationException("boom");

        var act = async () => await middleware.InvokeAsync(
            CreateContext(cancellationToken: TestContext.Current.CancellationToken),
            _ => throw exception
        );

        await act.Should().ThrowAsync<InvalidOperationException>();
        recorder.ConsumedMessages.Should().ContainSingle().Which.Should().Contain(
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
        activity
           .GetTagItem(MessagingSemanticConventions.ErrorType)
           .Should().Be(MessagingSemanticConventions.ErrorTypeOther);
        activity.Events.Should().ContainSingle(@event => @event.Name == "exception");
    }

    [Fact]
    public async Task InvokeAsync_RecordsCancellationWithoutErrorType()
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
           .ConsumedMessages.Should().ContainSingle().Which.Should()
           .NotContain(tag => tag.Key == MessagingSemanticConventions.ErrorType);
        recorder
           .Durations.Should().ContainSingle().Which.Should()
           .NotContain(tag => tag.Key == MessagingSemanticConventions.ErrorType);
        recorder
           .StartedActivities.Should().ContainSingle()
           .Which.GetTagItem(MessagingSemanticConventions.ErrorType).Should().BeNull();
    }

    private static IncomingMessageContext CreateContext(
        IReadOnlyDictionary<string, object?>? headers = null,
        CancellationToken cancellationToken = default
    )
    {
        return new IncomingMessageContext(
            new TestTransportMessage(headers),
            CreateEndpoint(),
            EmptyServiceProvider.Instance,
            NoOpAcknowledgement.Instance,
            cancellationToken,
            typeof(TestMessage)
        );
    }

    private static InboundEndpoint CreateEndpoint()
    {
        return new InboundEndpoint<TestMessage>(
            "endpoint",
            "test",
            Topology.DefaultName,
            typeof(TestHandler),
            typeof(PayloadCodecMessageDeserializer),
            "tests.message",
            MessageHandlerInvocation.Create<TestMessage, TestHandler>()
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
        private readonly string? _routingKey;

        public TestTransportMessage(
            IReadOnlyDictionary<string, object?>? headers,
            string? messageId = null,
            string? routingKey = null
        )
            : base(
                "test",
                "source",
                ReadOnlyMemory<byte>.Empty,
                headers ?? new Dictionary<string, object?>(),
                messageId: messageId
            )
        {
            _routingKey = routingKey;
        }

        public override string? DestinationRoutingKey => _routingKey;
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
