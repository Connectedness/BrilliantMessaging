using System;
using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;
using BrilliantMessaging.Core.Messaging.Outbound;
using BrilliantMessaging.Transport.InMemory.Inbound;
using BrilliantMessaging.Transport.InMemory.Outbound;
using BrilliantMessaging.Transport.InMemory.Tests.TestSupport;
using FluentAssertions;
using Xunit;

namespace BrilliantMessaging.Transport.InMemory.Tests;

public sealed class InMemoryBuilderTests
{
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Topic_RejectsBlankTopic(string? topic)
    {
        InMemoryTopologyBuilder builder = new ();

        var act = () => builder.Topic(topic!);

        act.Should().Throw<ArgumentException>().WithParameterName("topic");
    }

    [Fact]
    public void Topic_RejectsDuplicateTopic()
    {
        InMemoryTopologyBuilder builder = new ();
        builder.Topic("orders");

        var act = () => builder.Topic("orders");

        act.Should().Throw<InvalidOperationException>().WithMessage("Topic 'orders' is already declared.");
    }

    [Fact]
    public void Publish_RejectsNullConfigure()
    {
        InMemoryTopologyBuilder builder = new ();

        var act = () => builder.Publish<OrderPlaced>(null!);

        act.Should().Throw<ArgumentNullException>().WithParameterName("configure");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Consume_RejectsBlankTopic(string? topic)
    {
        InMemoryTopologyBuilder builder = new ();

        var act = () => builder.Consume(topic!, static _ => { });

        act.Should().Throw<ArgumentException>().WithParameterName("topic");
    }

    [Fact]
    public void Consume_RejectsNullConfigure()
    {
        InMemoryTopologyBuilder builder = new ();

        var act = () => builder.Consume("orders", null!);

        act.Should().Throw<ArgumentNullException>().WithParameterName("configure");
    }

    [Fact]
    public void ShutdownTimeout_RejectsZeroTimeout()
    {
        InMemoryTopologyBuilder builder = new ();

        var act = () => builder.ShutdownTimeout(TimeSpan.Zero);

        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("timeout");
    }

    [Fact]
    public void ShutdownTimeout_AcceptsInfiniteTimeout()
    {
        InMemoryTopologyBuilder builder = new ();

        builder.ShutdownTimeout(Timeout.InfiniteTimeSpan);

        var configuration = ((IBuildable<InMemoryTopologyConfiguration>) builder).Build();
        configuration.ShutdownTimeout.Should().Be(Timeout.InfiniteTimeSpan);
    }

    [Fact]
    public void Build_DefaultsToUnboundedRecording()
    {
        InMemoryTopologyBuilder builder = new ();

        var configuration = ((IBuildable<InMemoryTopologyConfiguration>) builder).Build();

        configuration.Recording.Should().Be(InMemoryRecordingOptions.Unbounded);
    }

    [Fact]
    public void RecordMessages_ParameterlessConfiguresUnboundedRecording()
    {
        InMemoryTopologyBuilder builder = new ();

        builder.RecordMessages(false).RecordMessages();

        var configuration = ((IBuildable<InMemoryTopologyConfiguration>) builder).Build();
        configuration.Recording.Should().Be(InMemoryRecordingOptions.Unbounded);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void RecordMessages_BooleanConfiguresRecordingMode(bool record)
    {
        InMemoryTopologyBuilder builder = new ();

        builder.RecordMessages(record);

        var configuration = ((IBuildable<InMemoryTopologyConfiguration>) builder).Build();
        configuration.Recording.Should().Be(
            record ? InMemoryRecordingOptions.Unbounded : InMemoryRecordingOptions.Off
        );
    }

    [Fact]
    public void RecordMessages_MaxPerTopicConfiguresBoundedRecording()
    {
        InMemoryTopologyBuilder builder = new ();

        builder.RecordMessages(maxPerTopic: 3);

        var configuration = ((IBuildable<InMemoryTopologyConfiguration>) builder).Build();
        configuration.Recording.Should().Be(InMemoryRecordingOptions.Bounded(3));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void RecordMessages_RejectsInvalidMaxPerTopic(int maxPerTopic)
    {
        InMemoryTopologyBuilder builder = new ();

        var act = () => builder.RecordMessages(maxPerTopic);

        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("maxPerTopic");
    }

    [Fact]
    public void DirectionSpecificInterfaces_ForwardShutdownTimeout()
    {
        InMemoryTopologyBuilder outboundBuilder = new ();
        InMemoryTopologyBuilder inboundBuilder = new ();

        ((IInMemoryOutboundTopologyBuilder) outboundBuilder).ShutdownTimeout(TimeSpan.FromSeconds(1));
        ((IInMemoryInboundTopologyBuilder) inboundBuilder).ShutdownTimeout(TimeSpan.FromSeconds(2));

        ((IBuildable<InMemoryTopologyConfiguration>) outboundBuilder).Build().ShutdownTimeout
           .Should()
           .Be(TimeSpan.FromSeconds(1));
        ((IBuildable<InMemoryTopologyConfiguration>) inboundBuilder).Build().ShutdownTimeout
           .Should()
           .Be(TimeSpan.FromSeconds(2));
    }

    [Fact]
    public void DirectionSpecificInterfaces_ForwardRecordMessages()
    {
        InMemoryTopologyBuilder outboundBuilder = new ();
        InMemoryTopologyBuilder inboundBuilder = new ();

        ((IInMemoryOutboundTopologyBuilder) outboundBuilder).RecordMessages(false);
        ((IInMemoryInboundTopologyBuilder) inboundBuilder).RecordMessages(maxPerTopic: 2);

        ((IBuildable<InMemoryTopologyConfiguration>) outboundBuilder).Build().Recording
           .Should()
           .Be(InMemoryRecordingOptions.Off);
        ((IBuildable<InMemoryTopologyConfiguration>) inboundBuilder).Build().Recording
           .Should()
           .Be(InMemoryRecordingOptions.Bounded(2));
    }

    [Fact]
    public void OutboundTargetBuilder_RequiresTopic()
    {
        InMemoryOutboundTargetBuilder<OrderPlaced> builder = new ();

        var act = () => ((IBuildable<InMemoryOutboundTargetDefinition>) builder).Build();

        act.Should().Throw<InvalidOperationException>()
           .WithMessage("An in-memory outbound target must select a topic with ToTopic(...).");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void OutboundTargetBuilder_RejectsBlankTopic(string? topic)
    {
        InMemoryOutboundTargetBuilder<OrderPlaced> builder = new ();

        var act = () => builder.ToTopic(topic!);

        act.Should().Throw<ArgumentException>().WithParameterName("topic");
    }

    [Fact]
    public void OutboundTargetBuilder_CapturesSerializerOverride()
    {
        InMemoryOutboundTargetBuilder<OrderPlaced> builder = new ("orders-target");

        builder.ToTopic("orders").WithSerializer<CloudEventMessageSerializer>();

        var definition = ((IBuildable<InMemoryOutboundTargetDefinition>) builder).Build();
        definition.Should().Be(
            new InMemoryOutboundTargetDefinition(
                typeof(OrderPlaced),
                "orders",
                "orders-target",
                typeof(CloudEventMessageSerializer)
            )
        );
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void InboundConsumerBuilder_RejectsInvalidConcurrency(int concurrency)
    {
        InMemoryInboundConsumerBuilder builder = new ("orders");

        var act = () => builder.Concurrency(concurrency);

        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("concurrency");
    }

    [Fact]
    public void InboundConsumerBuilder_RejectsNullFailureConfiguration()
    {
        InMemoryInboundConsumerBuilder builder = new ("orders");

        var act = () => builder.OnFailure(null!);

        act.Should().Throw<ArgumentNullException>().WithParameterName("configure");
    }

    [Fact]
    public void InboundConsumerBuilder_RejectsAbstractHandlerType()
    {
        InMemoryInboundConsumerBuilder builder = new ("orders");

        var act = () => builder.Handle<OrderPlaced, AbstractOrderPlacedHandler>();

        act.Should().Throw<ArgumentException>().WithParameterName("THandler");
    }

    [Fact]
    public void InboundConsumerBuilder_RequiresTextTopic()
    {
        var act = () => new InMemoryInboundConsumerBuilder(" ");

        act.Should().Throw<ArgumentException>().WithParameterName("topic");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void RetryPolicyBuilder_RejectsInvalidMaxAttempts(int maxAttempts)
    {
        InMemoryRetryPolicyBuilder builder = new ();

        var act = () => builder.MaxAttempts(maxAttempts);

        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("maxAttempts");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void RetryPolicyBuilder_RejectsInvalidBackoffDelay(int milliseconds)
    {
        InMemoryRetryPolicyBuilder builder = new ();

        var linear = () => builder.LinearBackoff(TimeSpan.FromMilliseconds(milliseconds));
        var exponential = () => builder.ExponentialBackoff(TimeSpan.FromMilliseconds(milliseconds));

        linear.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("delay");
        exponential.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("delay");
    }

    [Fact]
    public void RetryPolicyBuilder_CapturesExponentialBackoff()
    {
        InMemoryRetryPolicyBuilder builder = new ();

        builder.MaxAttempts(4).ExponentialBackoff(TimeSpan.FromMilliseconds(5));

        var policy = ((IBuildable<InMemoryRetryPolicy>) builder).Build();
        policy.MaxAttempts.Should().Be(4);
        policy.Backoff.Kind.Should().Be(InMemoryBackoffKind.Exponential);
        policy.Backoff.GetDelay(1).Should().Be(TimeSpan.FromMilliseconds(5));
        policy.Backoff.GetDelay(2).Should().Be(TimeSpan.FromMilliseconds(10));
    }

    [Fact]
    public void DeliveryPolicyBuilder_RejectsNullRetryConfiguration()
    {
        InMemoryDeliveryPolicyBuilder builder = new ();

        var act = () => builder.Retry(null!);

        act.Should().Throw<ArgumentNullException>().WithParameterName("configure");
    }

    [Fact]
    public void DeliveryPolicyBuilder_RejectsBlankDeadLetterTopic()
    {
        InMemoryDeliveryPolicyBuilder builder = new ();

        var act = () => builder.DeadLetterTo(" ");

        act.Should().Throw<ArgumentException>().WithParameterName("topic");
    }

    [Fact]
    public void HandlerBuilder_CapturesManualAckAndDeserializerOverride()
    {
        InMemoryInboundHandlerBuilder builder = new ();

        builder.WithDeserializer<PayloadCodecMessageDeserializer>().ManualAck();

        var configuration = ((IBuildable<InMemoryInboundHandlerConfiguration>) builder).Build();
        configuration.DeserializerType.Should().Be(typeof(PayloadCodecMessageDeserializer));
        configuration.AckMode.Should().Be(MessageAckMode.Manual);
    }

    [Fact]
    public void HandlerBuilder_RejectsUndefinedAckMode()
    {
        InMemoryInboundHandlerBuilder builder = new ();

        var act = () => builder.WithAckMode((MessageAckMode) 99);

        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("ackMode");
    }
}

public abstract class AbstractOrderPlacedHandler : IMessageHandler<OrderPlaced>
{
    public abstract Task HandleAsync(
        OrderPlaced message,
        IncomingMessageContext context,
        CancellationToken cancellationToken
    );
}
