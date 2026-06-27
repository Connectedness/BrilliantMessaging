using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;
using BrilliantMessaging.Core.Tests.Messaging.TestSupport;
using FluentAssertions;
using Xunit;

namespace BrilliantMessaging.Core.Tests.Messaging;

public sealed class MessageDeserializationMiddlewareTests
{
    [Fact]
    public async Task InvokeAsync_DeserializesMissingMessageBeforeCallingNext()
    {
        var decoded = new SampleMessage("decoded");
        RecordingDeserializer deserializer = new (decoded);
        var services = new DictionaryServiceProvider().Add(deserializer);
        var context = CreateContext(services);
        MessageDeserializationMiddleware middleware = new ();

        await middleware.InvokeAsync(
            context,
            incoming =>
            {
                incoming.Message.Should().BeSameAs(decoded);
                incoming.Should().BeSameAs(context);
                return Task.CompletedTask;
            }
        );

        deserializer.Context.Should().BeSameAs(context);
    }

    [Fact]
    public async Task InvokeAsync_SkipsDeserializerWhenMessageAlreadyExists()
    {
        var existing = new SampleMessage("existing");
        DictionaryServiceProvider services = new ();
        var context = CreateContext(services);
        context.Message = existing;
        MessageDeserializationMiddleware middleware = new ();

        await middleware.InvokeAsync(
            context,
            incoming =>
            {
                incoming.Message.Should().BeSameAs(existing);
                return Task.CompletedTask;
            }
        );
    }

    [Fact]
    public async Task InvokeAsync_WrapsDeserializerFailure()
    {
        var failure = new FormatException("bad payload");
        var services = new DictionaryServiceProvider().Add(new RecordingDeserializer(failure));
        var context = CreateContext(services);
        MessageDeserializationMiddleware middleware = new ();

        var act = async () => await middleware.InvokeAsync(context, static _ => Task.CompletedTask);

        var exception = (await act.Should().ThrowAsync<MessageDeserializationException>()).Which;
        exception.MessageType.Should().Be(typeof(SampleMessage));
        exception.InnerException.Should().BeSameAs(failure);
    }

    [Fact]
    public async Task InvokeAsync_PreservesCancellationFailure()
    {
        var cancelledToken = new CancellationToken(canceled: true);
        var failure = new OperationCanceledException(cancelledToken);
        var services = new DictionaryServiceProvider().Add(new RecordingDeserializer(failure));
        var context = CreateContext(services, cancelledToken);
        MessageDeserializationMiddleware middleware = new ();

        var act = async () => await middleware.InvokeAsync(context, static _ => Task.CompletedTask);

        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task InvokeAsync_RejectsNullArguments()
    {
        MessageDeserializationMiddleware middleware = new ();
        var context = CreateContext(new DictionaryServiceProvider());

        var nullContext = async () => await middleware.InvokeAsync(null!, static _ => Task.CompletedTask);
        var nullNext = async () => await middleware.InvokeAsync(context, null!);

        await nullContext.Should().ThrowAsync<ArgumentNullException>().WithParameterName("context");
        await nullNext.Should().ThrowAsync<ArgumentNullException>().WithParameterName("next");
    }

    private static IncomingMessageContext CreateContext(
        IServiceProvider services,
        CancellationToken? cancellationToken = null
    )
    {
        var endpoint = new InboundEndpoint<SampleMessage>(
            "endpoint",
            "test",
            Topology.DefaultName,
            typeof(TestHandler),
            typeof(RecordingDeserializer),
            "tests.sample",
            MessageHandlerInvocation.Create<SampleMessage, TestHandler>()
        );

        return new IncomingMessageContext(
            new TestTransportMessage(),
            endpoint,
            services,
            new NoOpAcknowledgement(),
            cancellationToken ?? TestContext.Current.CancellationToken,
            typeof(SampleMessage)
        );
    }

    private sealed class RecordingDeserializer : IMessageDeserializer
    {
        private readonly object? _message;

        public RecordingDeserializer(object? message)
        {
            _message = message;
        }

        public IncomingMessageContext? Context { get; private set; }

        public ValueTask<object?> DeserializeAsync(
            IncomingMessageContext context,
            CancellationToken cancellationToken = default
        )
        {
            if (_message is Exception exception)
            {
                throw exception;
            }

            Context = context;
            return new ValueTask<object?>(_message);
        }
    }

    private sealed class TestHandler : IMessageHandler<SampleMessage>
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

    private sealed class TestTransportMessage : TransportMessage
    {
        public TestTransportMessage()
            : base("test", "source", ReadOnlyMemory<byte>.Empty, new Dictionary<string, object?>()) { }
    }

    private sealed class NoOpAcknowledgement : IMessageAcknowledgement
    {
        public Task AckAsync(CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }

        public Task NackAsync(bool requeue, CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }
    }
}
