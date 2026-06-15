using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Errors;
using Usf.Core.Messaging.Serialization;
using Xunit;

namespace Usf.Core.Tests.Messaging.Serialization;

public sealed class PayloadCodecMessageDeserializerTests
{
    [Fact]
    public async Task DeserializeAsync_DecodesTransportBodyForRequestedMessageType()
    {
        var expected = new TestMessage("decoded");
        RecordingPayloadCodec codec = new (expected);
        PayloadCodecMessageDeserializer deserializer = new (codec);
        var context = CreateContext("raw-payload"u8.ToArray());

        var message = await deserializer.DeserializeAsync(
            context,
            TestContext.Current.CancellationToken
        );

        message.Should().BeSameAs(expected);
        codec.DecodedData.ToArray().Should().Equal("raw-payload"u8.ToArray());
        codec.DecodedMessageType.Should().Be(typeof(TestMessage));
    }

    [Fact]
    public async Task DeserializeAsync_WrapsCodecFailure()
    {
        var failure = new InvalidOperationException("invalid payload");
        PayloadCodecMessageDeserializer deserializer = new (new ThrowingPayloadCodec(failure));
        var context = CreateContext("invalid"u8.ToArray());

        var action = async () => await deserializer.DeserializeAsync(context);

        var exception = await action.Should().ThrowAsync<MessageDeserializationException>();
        exception.Which.MessageType.Should().Be(typeof(TestMessage));
        exception.Which.InnerException.Should().BeSameAs(failure);
    }

    private static IncomingMessageContext CreateContext(ReadOnlyMemory<byte> body)
    {
        var endpoint = new InboundEndpoint<TestMessage>(
            "endpoint",
            "test",
            Topology.DefaultName,
            typeof(TestHandler),
            typeof(PayloadCodecMessageDeserializer),
            "tests.message",
            MessageHandlerInvocation.Create<TestMessage, TestHandler>()
        );
        return new IncomingMessageContext(
            new TestTransportMessage(body),
            endpoint,
            EmptyServiceProvider.Instance,
            new NoOpAcknowledgement(),
            default,
            typeof(TestMessage)
        );
    }

    private sealed record TestMessage(string Value);

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
        public TestTransportMessage(ReadOnlyMemory<byte> body)
            : base("test", "source", body, new Dictionary<string, object?>()) { }
    }

    private sealed class RecordingPayloadCodec : IPayloadCodec
    {
        private readonly object _result;

        public RecordingPayloadCodec(object result)
        {
            _result = result;
        }

        public ReadOnlyMemory<byte> DecodedData { get; private set; }

        public Type? DecodedMessageType { get; private set; }

        public EncodedPayload Encode<T>(T message)
        {
            throw new NotSupportedException();
        }

        public object? Decode(ReadOnlyMemory<byte> data, Type messageType)
        {
            DecodedData = data;
            DecodedMessageType = messageType;
            return _result;
        }
    }

    private sealed class ThrowingPayloadCodec : IPayloadCodec
    {
        private readonly Exception _exception;

        public ThrowingPayloadCodec(Exception exception)
        {
            _exception = exception;
        }

        public EncodedPayload Encode<T>(T message)
        {
            throw new NotSupportedException();
        }

        public object? Decode(ReadOnlyMemory<byte> data, Type messageType)
        {
            throw _exception;
        }
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
}
