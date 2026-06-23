using System;
using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;
using BrilliantMessaging.Core.Tests.Messaging.TestSupport;
using FluentAssertions;
using Xunit;

namespace BrilliantMessaging.Core.Tests.Messaging;

public sealed class InboundEndpointGuardTests
{
    [Theory]
    [InlineData("name")]
    [InlineData("transportName")]
    [InlineData("topologyName")]
    [InlineData("discriminator")]
    public void Constructor_RejectsBlankTextArguments(string parameterName)
    {
        var act = () => CreateEndpoint(parameterName, " ");

        act.Should().Throw<ArgumentException>().WithParameterName(parameterName);
    }

    [Theory]
    [InlineData("handlerType")]
    [InlineData("deserializerType")]
    [InlineData("handlerInvocation")]
    public void Constructor_RejectsNullReferenceArguments(string parameterName)
    {
        var act = () => CreateEndpoint(parameterName, null);

        act.Should().Throw<ArgumentNullException>().WithParameterName(parameterName);
    }

    [Fact]
    public void Constructor_RejectsDeserializerThatDoesNotImplementDeserializerContract()
    {
        var act = () => new InboundEndpoint<SampleMessage>(
            "endpoint",
            "test",
            Topology.DefaultName,
            typeof(TestHandler),
            typeof(string),
            "tests.sample",
            static _ => Task.CompletedTask
        );

        act.Should().Throw<ArgumentException>().WithParameterName("deserializerType");
    }

    [Fact]
    public void Constructor_RejectsUnsupportedAcknowledgementMode()
    {
        var act = () => new InboundEndpoint<SampleMessage>(
            "endpoint",
            "test",
            Topology.DefaultName,
            typeof(TestHandler),
            typeof(PayloadCodecMessageDeserializer),
            "tests.sample",
            static _ => Task.CompletedTask,
            (MessageAckMode) 999
        );

        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("ackMode");
    }

    [Fact]
    public void Constructor_RejectsHandlerThatDoesNotHandleEndpointMessageType()
    {
        var act = () => new InboundEndpoint<SampleMessage>(
            "endpoint",
            "test",
            Topology.DefaultName,
            typeof(OtherHandler),
            typeof(PayloadCodecMessageDeserializer),
            "tests.sample",
            static _ => Task.CompletedTask
        );

        act.Should().Throw<ArgumentException>().WithParameterName("handlerType");
    }

    private static InboundEndpoint<SampleMessage> CreateEndpoint(string parameterName, object? value)
    {
        var name = parameterName == "name" ? (string) value! : "endpoint";
        var transportName = parameterName == "transportName" ? (string) value! : "test";
        var topologyName = parameterName == "topologyName" ? (string) value! : Topology.DefaultName;
        var handlerType = parameterName == "handlerType" ? (Type) value! : typeof(TestHandler);
        var deserializerType = parameterName == "deserializerType" ?
            (Type) value! :
            typeof(PayloadCodecMessageDeserializer);
        var discriminator = parameterName == "discriminator" ? (string) value! : "tests.sample";
        var handlerInvocation = parameterName == "handlerInvocation" ?
            (MessageDelegate) value! :
            static _ => Task.CompletedTask;

        return new InboundEndpoint<SampleMessage>(
            name,
            transportName,
            topologyName,
            handlerType,
            deserializerType,
            discriminator,
            handlerInvocation
        );
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

    private sealed class OtherHandler : IMessageHandler<OtherMessage>
    {
        public Task HandleAsync(
            OtherMessage message,
            IncomingMessageContext context,
            CancellationToken cancellationToken
        )
        {
            return Task.CompletedTask;
        }
    }
}
