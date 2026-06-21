using System;
using FluentAssertions;
using RabbitMQ.Client;
using Xunit;

namespace Bmf.Transport.RabbitMq.Tests.Unit;

public sealed class RabbitMqDeclarationBuilderTests
{
    [Fact]
    public void QueueBuilder_BuildsAllQueueDeclarationOptions()
    {
        var definition = new RabbitMqQueueBuilder("work")
           .WithDeclareMode(RabbitMqDeclareMode.Passive)
           .DurableQueue(false)
           .ExclusiveQueue()
           .AutoDeleteQueue()
           .WithArgument("x-custom", "custom")
           .WithDeadLetterExchange("dead")
           .WithDeadLetterRoutingKey(null!)
           .WithMessageTtl(TimeSpan.FromSeconds(5))
           .WithExpires(TimeSpan.FromMinutes(1))
           .WithMaxLength(100)
           .WithMaxLengthBytes(4096)
           .WithQueueType("classic")
           .AsQuorumQueue()
           .SingleActiveConsumer()
           .Build();

        definition.Name.Should().Be("work");
        definition.DeclareMode.Should().Be(RabbitMqDeclareMode.Passive);
        definition.Durable.Should().BeFalse();
        definition.Exclusive.Should().BeTrue();
        definition.AutoDelete.Should().BeTrue();
        definition.Arguments.Should().Contain("x-custom", "custom");
        definition.Arguments.Should().Contain("x-dead-letter-exchange", "dead");
        definition.Arguments.Should().Contain("x-dead-letter-routing-key", string.Empty);
        definition.Arguments.Should().Contain("x-message-ttl", 5000L);
        definition.Arguments.Should().Contain("x-expires", 60000L);
        definition.Arguments.Should().Contain("x-max-length", 100L);
        definition.Arguments.Should().Contain("x-max-length-bytes", 4096L);
        definition.Arguments.Should().Contain("x-queue-type", "quorum");
        definition.Arguments.Should().Contain("x-single-active-consumer", true);
    }

    [Fact]
    public void ExchangeBuilder_BuildsAllExchangeDeclarationOptions()
    {
        var definition = new RabbitMqExchangeBuilder("delayed", ExchangeType.Headers)
           .WithDeclareMode(RabbitMqDeclareMode.Passive)
           .DurableExchange(false)
           .AutoDeleteExchange()
           .WithArgument("x-custom", "custom")
           .WithAlternateExchange("alternate")
           .AsDelayedMessageExchange(ExchangeType.Topic)
           .Build();

        definition.Name.Should().Be("delayed");
        definition.Type.Should().Be(ExchangeType.Headers);
        definition.DeclareMode.Should().Be(RabbitMqDeclareMode.Passive);
        definition.Durable.Should().BeFalse();
        definition.AutoDelete.Should().BeTrue();
        definition.Arguments.Should().Contain("x-custom", "custom");
        definition.Arguments.Should().Contain("alternate-exchange", "alternate");
        definition.Arguments.Should().Contain("x-delayed-type", ExchangeType.Topic);
    }

    [Fact]
    public void QueueBuilder_RejectsInvalidArguments()
    {
        var builder = new RabbitMqQueueBuilder("work");

        Action blankArgument = () => builder.WithArgument(" ", "value");
        Action blankDeadLetterExchange = () => builder.WithDeadLetterExchange(" ");
        Action negativeMessageTtl = () => builder.WithMessageTtl(TimeSpan.FromMilliseconds(-1));
        Action negativeExpires = () => builder.WithExpires(TimeSpan.FromMilliseconds(-1));
        Action negativeMaxLength = () => builder.WithMaxLength(-1);
        Action negativeMaxLengthBytes = () => builder.WithMaxLengthBytes(-1);
        Action blankQueueType = () => builder.WithQueueType(" ");

        blankArgument.Should().Throw<ArgumentException>().WithParameterName("name");
        blankDeadLetterExchange.Should().Throw<ArgumentException>().WithParameterName("exchangeName");
        negativeMessageTtl.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("timeToLive");
        negativeExpires.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("expires");
        negativeMaxLength.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("maxLength");
        negativeMaxLengthBytes.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("maxLengthBytes");
        blankQueueType.Should().Throw<ArgumentException>().WithParameterName("queueType");
    }

    [Fact]
    public void ExchangeBuilder_RejectsInvalidArguments()
    {
        var builder = new RabbitMqExchangeBuilder("exchange", ExchangeType.Direct);

        Action blankArgument = () => builder.WithArgument(" ", "value");
        Action blankAlternateExchange = () => builder.WithAlternateExchange(" ");
        Action blankDelayedExchangeType = () => builder.AsDelayedMessageExchange(" ");

        blankArgument.Should().Throw<ArgumentException>().WithParameterName("name");
        blankAlternateExchange.Should().Throw<ArgumentException>().WithParameterName("exchangeName");
        blankDelayedExchangeType.Should().Throw<ArgumentException>().WithParameterName("delayedExchangeType");
    }
}
