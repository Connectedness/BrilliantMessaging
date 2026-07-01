using System;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;
using BrilliantMessaging.Core.Messaging.Outbound;
using BrilliantMessaging.Transport.Nats.Inbound;
using BrilliantMessaging.Transport.Nats.Outbound;
using BrilliantMessaging.Transport.Nats.Tests.TestSupport;
using FluentAssertions;
using Xunit;

namespace BrilliantMessaging.Transport.Nats.Tests.Unit;

public sealed class NatsBuilderTests
{
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void UseServer_RejectsBlankServer(string? serverUrl)
    {
        NatsTopologyBuilder builder = new ();

        var act = () => builder.UseServer(serverUrl!);

        act.Should().Throw<ArgumentException>().WithParameterName("serverUrl");
    }

    [Fact]
    public void AddNatsInboundTopology_DefaultNameDoesNotCollideWithOutboundDefaultName()
    {
        NatsTopology.DefaultInboundName.Should().NotBe(Topology.DefaultName);
    }

    [Fact]
    public void DirectionSpecificInterfaces_OnlyExposeRelevantMembers()
    {
        typeof(INatsOutboundTopologyBuilder)
           .GetMethod(nameof(INatsOutboundTopologyBuilder.Publish))
           .Should()
           .NotBeNull();
        typeof(INatsOutboundTopologyBuilder)
           .GetMethod("Consume")
           .Should()
           .BeNull();
        typeof(INatsInboundTopologyBuilder)
           .GetMethod(nameof(INatsInboundTopologyBuilder.Consume))
           .Should()
           .NotBeNull();
        typeof(INatsInboundTopologyBuilder)
           .GetMethod("Publish")
           .Should()
           .BeNull();
    }

    [Fact]
    public void OutboundTargetBuilder_CapturesSubjectSerializerAndDeduplication()
    {
        NatsOutboundTargetBuilder<OrderPlaced> builder = new ("orders-target");

        builder.ToSubject("orders.placed")
           .WithSerializer<CloudEventMessageSerializer>()
           .UseMessageIdDeduplication();

        var definition = ((IBuildable<NatsOutboundTargetDefinition>) builder).Build();
        definition.Should().Be(
            new NatsOutboundTargetDefinition(
                typeof(OrderPlaced),
                "orders.placed",
                "orders-target",
                typeof(CloudEventMessageSerializer),
                true
            )
        );
    }

    [Fact]
    public void OutboundTargetBuilder_RequiresSubject()
    {
        NatsOutboundTargetBuilder<OrderPlaced> builder = new ();

        var act = () => ((IBuildable<NatsOutboundTargetDefinition>) builder).Build();

        act.Should().Throw<InvalidOperationException>()
           .WithMessage("A NATS outbound target must select a subject with ToSubject(...).");
    }

    [Fact]
    public void InboundConsumerBuilder_CapturesJetStreamPolicyKnobsAndHandlerOptions()
    {
        NatsInboundConsumerBuilder builder = new ("ORDERS", "orders-worker");

        builder.FilterSubject("orders.placed")
           .Concurrency(3)
           .AckWait(TimeSpan.FromSeconds(10))
           .MaxDeliver(7)
           .MaxAckPending(99)
           .DeadLetterSubject("orders.dead")
           .Handle<OrderPlaced, OrderPlacedHandler>(
                handler => handler.ManualAck().WithDeserializer<PayloadCodecMessageDeserializer>()
            );

        var definition = ((IBuildable<NatsInboundConsumerDefinition>) builder).Build();

        definition.StreamName.Should().Be("ORDERS");
        definition.DurableName.Should().Be("orders-worker");
        definition.FilterSubject.Should().Be("orders.placed");
        definition.Concurrency.Should().Be(3);
        definition.AckWait.Should().Be(TimeSpan.FromSeconds(10));
        definition.MaxDeliver.Should().Be(7);
        definition.MaxAckPending.Should().Be(99);
        definition.DeadLetterSubject.Should().Be("orders.dead");
        var handler = definition.Handlers.Should().ContainSingle().Which;
        handler.MessageType.Should().Be<OrderPlaced>();
        handler.HandlerType.Should().Be<OrderPlacedHandler>();
        handler.AckMode.Should().Be(MessageAckMode.Manual);
        handler.DeserializerType.Should().Be<PayloadCodecMessageDeserializer>();
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void InboundConsumerBuilder_RejectsInvalidConcurrency(int concurrency)
    {
        NatsInboundConsumerBuilder builder = new ("ORDERS", "orders-worker");

        var act = () => builder.Concurrency(concurrency);

        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("concurrency");
    }
}
