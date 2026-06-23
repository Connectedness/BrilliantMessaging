using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;
using Xunit;

namespace BrilliantMessaging.Core.Tests.Messaging;

public sealed class IncomingMessageContextTests
{
    [Fact]
    public void Items_AreStoredWithStronglyTypedKeys()
    {
        var key = new MessageContextKey<int>("count");
        IncomingMessageContextItems items = new ();

        items.TryGetItem(key, out var missing).Should().BeFalse();
        missing.Should().Be(0);

        items.SetItem(key, 42);

        items.TryGetItem(key, out var value).Should().BeTrue();
        value.Should().Be(42);
        items.GetRequiredItem(key).Should().Be(42);
        items.RemoveItem(key).Should().BeTrue();
        items.TryGetItem(key, out _).Should().BeFalse();
    }

    [Fact]
    public void Constructor_PopulatesIdentityAndAdoptsMutableItems()
    {
        var key = new MessageContextKey<string>("seed");
        IncomingMessageContextItems items = new ();
        items.SetItem(key, "inspected");
        var transport = new TestTransportMessage();
        var endpoint = CreateEndpoint();
        var services = EmptyServiceProvider.Instance;
        var acknowledgement = new RecordingAcknowledgement();

        var context = new IncomingMessageContext(
            transport,
            endpoint,
            services,
            acknowledgement,
            TestContext.Current.CancellationToken,
            typeof(TestMessage),
            items
        );

        context.Transport.Should().BeSameAs(transport);
        context.Endpoint.Should().BeSameAs(endpoint);
        context.Services.Should().BeSameAs(services);
        context.Acknowledgement.Should().BeSameAs(acknowledgement);
        context.CancellationToken.Should().Be(TestContext.Current.CancellationToken);
        context.MessageType.Should().Be(typeof(TestMessage));
        context.Items.Should().BeSameAs(items);
        context.Items.GetRequiredItem(key).Should().Be("inspected");

        var message = new TestMessage();
        context.Message = message;
        context.Items.SetItem(key, "middleware");

        context.Message.Should().BeSameAs(message);
        items.GetRequiredItem(key).Should().Be("middleware");

        typeof(IncomingMessageContext).GetProperty(nameof(IncomingMessageContext.Transport))!
           .SetMethod.Should().BeNull();
        typeof(IncomingMessageContext).GetProperty(nameof(IncomingMessageContext.Endpoint))!
           .SetMethod.Should().BeNull();
        typeof(IncomingMessageContext).GetProperty(nameof(IncomingMessageContext.Services))!
           .SetMethod.Should().BeNull();
        typeof(IncomingMessageContext).GetProperty(nameof(IncomingMessageContext.Acknowledgement))!
           .SetMethod.Should().BeNull();
        typeof(IncomingMessageContext).GetProperty(nameof(IncomingMessageContext.CancellationToken))!
           .SetMethod.Should().BeNull();
        typeof(IncomingMessageContext).GetProperty(nameof(IncomingMessageContext.MessageType))!
           .SetMethod.Should().BeNull();
        typeof(IncomingMessageContext).GetProperty(nameof(IncomingMessageContext.Message))!
           .SetMethod.Should().NotBeNull();
    }

    private static InboundEndpoint<TestMessage> CreateEndpoint()
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
        public TestTransportMessage()
            : base(
                "test",
                "source",
                ReadOnlyMemory<byte>.Empty,
                new Dictionary<string, object?>()
            ) { }
    }

    private sealed class RecordingAcknowledgement : IMessageAcknowledgement
    {
        public Task AckAsync(
            CancellationToken cancellationToken = default
        )
        {
            return Task.CompletedTask;
        }

        public Task NackAsync(
            bool requeue,
            CancellationToken cancellationToken = default
        )
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
