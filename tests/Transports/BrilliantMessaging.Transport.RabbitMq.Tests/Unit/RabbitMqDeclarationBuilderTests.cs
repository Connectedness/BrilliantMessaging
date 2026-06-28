using System;
using BrilliantMessaging.Transport.RabbitMq.Tests.TestSupport;
using FluentAssertions;
using RabbitMQ.Client;
using Xunit;

namespace BrilliantMessaging.Transport.RabbitMq.Tests.Unit;

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
           .AsClassicQueue()
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
        definition.Arguments.Should().Contain("x-queue-type", "classic");
        definition.Arguments.Should().Contain("x-single-active-consumer", true);
    }

    [Fact]
    public void QueueBuilder_DefaultsToQuorumQueue()
    {
        var definition = new RabbitMqQueueBuilder("work").Build();

        definition.Durable.Should().BeTrue();
        definition.Arguments.Should().Contain("x-queue-type", "quorum");
    }

    [Fact]
    public void QueueBuilder_AsQuorumQueueIsIdempotent()
    {
        var definition = new RabbitMqQueueBuilder("work")
           .AsQuorumQueue()
           .AsQuorumQueue()
           .Build();

        definition.Arguments.Should().Contain("x-queue-type", "quorum");
    }

    [Fact]
    public void QueueBuilder_UseDefaultQueueType_RemovesQueueTypeArgument()
    {
        var definition = new RabbitMqQueueBuilder("work")
           .UseDefaultQueueType()
           .Build();

        definition.Arguments.Should().NotContainKey("x-queue-type");
    }

    [Fact]
    public void QueueBuilder_UseDefaultQueueType_IsIdempotent()
    {
        var definition = new RabbitMqQueueBuilder("work")
           .UseDefaultQueueType()
           .UseDefaultQueueType()
           .Build();

        definition.Arguments.Should().NotContainKey("x-queue-type");
    }

    [Fact]
    public void QueueBuilder_UseDefaultQueueType_OverridesPreviouslySetQueueType()
    {
        var definition = new RabbitMqQueueBuilder("work")
           .AsClassicQueue()
           .UseDefaultQueueType()
           .Build();

        definition.Arguments.Should().NotContainKey("x-queue-type");
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
        Action negativeDeliveryLimit = () => builder.WithDeliveryLimit(-2);
        Action negativeDelayedRetryMin = () => builder.WithDelayedRetry(TimeSpan.FromMilliseconds(-1));
        Action zeroMaxPriority = () => builder.WithMaxPriority(0);
        Action zeroInitialClusterSize = () => builder.WithInitialClusterSize(0);
        Action zeroConsumerTimeout = () => builder.WithConsumerTimeout(TimeSpan.Zero);
        Action negativeConsumerTimeout = () => builder.WithConsumerTimeout(TimeSpan.FromMilliseconds(-1));
        Action negativeDelayedRetryMax = () => builder.WithDelayedRetry(
            RabbitMqDelayedRetryType.All,
            TimeSpan.FromSeconds(1),
            TimeSpan.FromMilliseconds(-1)
        );

        blankArgument.Should().Throw<ArgumentException>().WithParameterName("name");
        blankDeadLetterExchange.Should().Throw<ArgumentException>().WithParameterName("exchangeName");
        negativeMessageTtl.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("timeToLive");
        negativeExpires.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("expires");
        negativeMaxLength.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("maxLength");
        negativeMaxLengthBytes.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("maxLengthBytes");
        blankQueueType.Should().Throw<ArgumentException>().WithParameterName("queueType");
        negativeDeliveryLimit.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("limit");
        negativeDelayedRetryMin.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("minDelay");
        zeroMaxPriority.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("maxPriority");
        zeroInitialClusterSize.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("size");
        zeroConsumerTimeout.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("timeout");
        negativeConsumerTimeout.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("timeout");
        negativeDelayedRetryMax.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("maxDelay");
    }

    [Fact]
    public void QueueBuilder_WritesDeliveryLimitArgument()
    {
        var definition = new RabbitMqQueueBuilder("work")
           .WithDeliveryLimit(5)
           .Build();

        definition.Arguments.Should().Contain("x-delivery-limit", 5);
    }

    [Fact]
    public void QueueBuilder_AllowsDeliveryLimitMinusOne()
    {
        var definition = new RabbitMqQueueBuilder("work")
           .WithDeliveryLimit(-1)
           .Build();

        definition.Arguments.Should().Contain("x-delivery-limit", -1);
    }

    [Fact]
    public void QueueBuilder_WritesDelayedRetryWithTypeAndMinMax()
    {
        var definition = new RabbitMqQueueBuilder("work")
           .WithDelayedRetry(RabbitMqDelayedRetryType.Failed, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(10))
           .Build();

        definition.Arguments.Should().Contain("x-delayed-retry-type", "failed");
        definition.Arguments.Should().Contain("x-delayed-retry-min", 1000L);
        definition.Arguments.Should().Contain("x-delayed-retry-max", 10000L);
    }

    [Fact]
    public void QueueBuilder_WritesDelayedRetryWithDefaultTypeAndNoMax()
    {
        var definition = new RabbitMqQueueBuilder("work")
           .WithDelayedRetry(TimeSpan.FromSeconds(2))
           .Build();

        definition.Arguments.Should().Contain("x-delayed-retry-type", "all");
        definition.Arguments.Should().Contain("x-delayed-retry-min", 2000L);
        definition.Arguments.Should().NotContainKey("x-delayed-retry-max");
    }

    [Fact]
    public void QueueBuilder_WritesDeadLetterStrategyArgument()
    {
        var definition = new RabbitMqQueueBuilder("work")
           .WithDeadLetterStrategy(RabbitMqDeadLetterStrategy.AtLeastOnce)
           .Build();

        definition.Arguments.Should().Contain("x-dead-letter-strategy", "at-least-once");
    }

    [Fact]
    public void QueueBuilder_WritesOverflowArgument()
    {
        var definition = new RabbitMqQueueBuilder("work")
           .WithOverflow(RabbitMqOverflow.RejectPublishDlx)
           .Build();

        definition.Arguments.Should().Contain("x-overflow", "reject-publish-dlx");
    }

    [Fact]
    public void QueueBuilder_WritesMaxPriorityArgument()
    {
        var definition = new RabbitMqQueueBuilder("work")
           .AsClassicQueue()
           .WithMaxPriority(10)
           .Build();

        definition.Arguments.Should().Contain("x-max-priority", (byte) 10);
    }

    [Fact]
    public void QueueBuilder_WritesQueueLeaderLocatorArgument()
    {
        var definition = new RabbitMqQueueBuilder("work")
           .WithQueueLeaderLocator(RabbitMqQueueLeaderLocator.Balanced)
           .Build();

        definition.Arguments.Should().Contain("x-queue-leader-locator", "balanced");
    }

    [Fact]
    public void QueueBuilder_WritesInitialClusterSizeArgument()
    {
        var definition = new RabbitMqQueueBuilder("work")
           .WithInitialClusterSize(3)
           .Build();

        definition.Arguments.Should().Contain("x-quorum-initial-group-size", 3);
    }

    [Fact]
    public void QueueBuilder_WritesConsumerTimeoutArgument()
    {
        var definition = new RabbitMqQueueBuilder("work")
           .WithConsumerTimeout(TimeSpan.FromSeconds(30))
           .Build();

        definition.Arguments.Should().Contain("x-consumer-timeout", 30000L);
    }

    [Fact]
    public void QueueBuilder_WritesAllDelayedRetryTypeValues()
    {
        var disabled = new RabbitMqQueueBuilder("work")
           .WithDelayedRetry(RabbitMqDelayedRetryType.Disabled, TimeSpan.FromSeconds(1))
           .Build();
        var returned = new RabbitMqQueueBuilder("work")
           .WithDelayedRetry(RabbitMqDelayedRetryType.Returned, TimeSpan.FromSeconds(1))
           .Build();

        disabled.Arguments.Should().Contain("x-delayed-retry-type", "disabled");
        returned.Arguments.Should().Contain("x-delayed-retry-type", "returned");
    }

    [Fact]
    public void QueueBuilder_WritesAllDeadLetterStrategyValues()
    {
        var atMostOnce = new RabbitMqQueueBuilder("work")
           .WithDeadLetterStrategy(RabbitMqDeadLetterStrategy.AtMostOnce)
           .Build();

        atMostOnce.Arguments.Should().Contain("x-dead-letter-strategy", "at-most-once");
    }

    [Fact]
    public void QueueBuilder_WritesAllOverflowValues()
    {
        var dropHead = new RabbitMqQueueBuilder("work")
           .WithOverflow(RabbitMqOverflow.DropHead)
           .Build();
        var rejectPublish = new RabbitMqQueueBuilder("work")
           .WithOverflow(RabbitMqOverflow.RejectPublish)
           .Build();

        dropHead.Arguments.Should().Contain("x-overflow", "drop-head");
        rejectPublish.Arguments.Should().Contain("x-overflow", "reject-publish");
    }

    [Fact]
    public void QueueBuilder_WithDelayedRetry_UnsetsMaxWhenNull()
    {
        var definition = new RabbitMqQueueBuilder("work")
            .WithDelayedRetry(RabbitMqDelayedRetryType.All, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(10))
            .WithDelayedRetry(RabbitMqDelayedRetryType.All, TimeSpan.FromSeconds(1))
            .Build();

        definition.Arguments.Should().NotContainKey("x-delayed-retry-max");
    }

    [Fact]
    public void QueueBuilder_WritesAllQueueLeaderLocatorValues()
    {
        var clientLocal = new RabbitMqQueueBuilder("work")
           .WithQueueLeaderLocator(RabbitMqQueueLeaderLocator.ClientLocal)
           .Build();

        clientLocal.Arguments.Should().Contain("x-queue-leader-locator", "client-local");
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
