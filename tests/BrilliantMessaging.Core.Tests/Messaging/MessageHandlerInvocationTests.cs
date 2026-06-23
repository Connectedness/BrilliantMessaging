using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;
using Xunit;

namespace BrilliantMessaging.Core.Tests.Messaging;

public sealed class MessageHandlerInvocationTests
{
    [Fact]
    public async Task InvokeHandlerAsync_ResolvesConcreteHandlerAndPassesMessageContextAndCancellationToken()
    {
        var handler = new RecordingHandler();
        var serviceProvider = new RecordingServiceProvider(handler);
        var endpoint = CreateEndpoint();
        var message = new TestMessage("value");
        var context = CreateContext(endpoint, serviceProvider, TestContext.Current.CancellationToken);
        context.Message = message;

        await endpoint.InvokeHandlerAsync(context);

        handler.Message.Should().BeSameAs(message);
        handler.Context.Should().BeSameAs(context);
        handler.CancellationToken.Should().Be(TestContext.Current.CancellationToken);
    }

    [Fact]
    public void InvokeHandlerAsync_RejectsNullContext()
    {
        var endpoint = CreateEndpoint();

        Action act = () => _ = endpoint.InvokeHandlerAsync(null!);

        act.Should().Throw<ArgumentNullException>().WithParameterName("context");
    }

    [Fact]
    public void InvokeHandlerAsync_RejectsContextWithoutDeserializedMessage()
    {
        var endpoint = CreateEndpoint();
        var context = CreateContext(
            endpoint,
            EmptyServiceProvider.Instance,
            TestContext.Current.CancellationToken
        );

        Action act = () => _ = endpoint.InvokeHandlerAsync(context);

        act.Should().Throw<InvalidOperationException>()
           .WithMessage("The inbound message has not been deserialized.");
    }

    private static InboundEndpoint<TestMessage> CreateEndpoint()
    {
        return new InboundEndpoint<TestMessage>(
            "endpoint",
            "test",
            Topology.DefaultName,
            typeof(RecordingHandler),
            typeof(PayloadCodecMessageDeserializer),
            "tests.message",
            MessageHandlerInvocation.Create<TestMessage, RecordingHandler>()
        );
    }

    private static IncomingMessageContext CreateContext(
        InboundEndpoint endpoint,
        IServiceProvider services,
        CancellationToken cancellationToken
    )
    {
        return new IncomingMessageContext(
            new TestTransportMessage(),
            endpoint,
            services,
            new NoOpAcknowledgement(),
            cancellationToken,
            typeof(TestMessage)
        );
    }

    private sealed record TestMessage(string Value);

    private sealed class RecordingHandler : IMessageHandler<TestMessage>
    {
        public TestMessage? Message { get; private set; }

        public IncomingMessageContext? Context { get; private set; }

        public CancellationToken CancellationToken { get; private set; }

        public Task HandleAsync(
            TestMessage message,
            IncomingMessageContext context,
            CancellationToken cancellationToken
        )
        {
            Message = message;
            Context = context;
            CancellationToken = cancellationToken;
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

    private sealed class EmptyServiceProvider : IServiceProvider
    {
        private EmptyServiceProvider() { }

        public static EmptyServiceProvider Instance { get; } = new ();

        public object? GetService(Type serviceType)
        {
            return null;
        }
    }

    private sealed class RecordingServiceProvider : IServiceProvider
    {
        private readonly RecordingHandler _handler;

        public RecordingServiceProvider(RecordingHandler handler)
        {
            _handler = handler;
        }

        public object? GetService(Type serviceType)
        {
            return serviceType == typeof(RecordingHandler) ? _handler : null;
        }
    }
}
