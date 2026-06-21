using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Bmf.Core.Messaging;
using Bmf.Core.Messaging.Inbound;
using Bmf.Core.Tests.Messaging.TestSupport;
using FluentAssertions;
using Xunit;

namespace Bmf.Core.Tests.Messaging;

public sealed class MessagePipelineBuilderTests
{
    [Fact]
    public async Task Build_ComposesMiddlewareInRegistrationOrder()
    {
        PipelineEvents events = new ();
        var services = new DictionaryServiceProvider()
           .Add(events)
           .Add(new RecordingMiddleware(events));
        var context = CreateContext(services);
        MessagePipelineBuilder builder = new ();
        builder.Use(
            async (incoming, next) =>
            {
                events.Values.Add("inline-before");
                await next(incoming);
                events.Values.Add("inline-after");
            }
        );
        builder.UseMiddleware<RecordingMiddleware>();
        var pipeline = builder.Build(
            incoming =>
            {
                incoming.Should().BeSameAs(context);
                events.Values.Add("terminal");
                return Task.CompletedTask;
            }
        );

        await pipeline(context);

        events.Values.Should().Equal("inline-before", "typed-before", "terminal", "typed-after", "inline-after");
    }

    [Fact]
    public void Use_RejectsNullMiddleware()
    {
        MessagePipelineBuilder builder = new ();

        var component = () => builder.Use((Func<MessageDelegate, MessageDelegate>) null!);
        var inline = () => builder.Use((Func<IncomingMessageContext, MessageDelegate, Task>) null!);

        component.Should().Throw<ArgumentNullException>().WithParameterName("middleware");
        inline.Should().Throw<ArgumentNullException>().WithParameterName("middleware");
    }

    [Fact]
    public void Build_RejectsNullTerminalDelegate()
    {
        MessagePipelineBuilder builder = new ();

        var act = () => builder.Build(null!);

        act.Should().Throw<ArgumentNullException>().WithParameterName("terminal");
    }

    private static IncomingMessageContext CreateContext(IServiceProvider services)
    {
        var endpoint = new InboundEndpoint<SampleMessage>(
            "endpoint",
            "test",
            Topology.DefaultName,
            typeof(TestHandler),
            typeof(PayloadCodecMessageDeserializer),
            "tests.sample",
            MessageHandlerInvocation.Create<SampleMessage, TestHandler>()
        );

        return new IncomingMessageContext(
            new TestTransportMessage(),
            endpoint,
            services,
            new NoOpAcknowledgement(),
            TestContext.Current.CancellationToken,
            typeof(SampleMessage)
        );
    }

    private sealed class RecordingMiddleware : IMessageMiddleware
    {
        private readonly PipelineEvents _events;

        public RecordingMiddleware(PipelineEvents events)
        {
            _events = events;
        }

        public async Task InvokeAsync(IncomingMessageContext context, MessageDelegate next)
        {
            _events.Values.Add("typed-before");
            await next(context);
            _events.Values.Add("typed-after");
        }
    }

    private sealed class PipelineEvents
    {
        public List<string> Values { get; } = [];
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
