using System;
using BrilliantMessaging.Transport.RabbitMq.Outbound;
using BrilliantMessaging.Transport.RabbitMq.Tests.TestSupport;
using FluentAssertions;
using RabbitMQ.Client;
using Xunit;

namespace BrilliantMessaging.Transport.RabbitMq.Tests.Unit;

public sealed class RabbitMqTopologyBuilderValidationTests
{
    [Fact]
    public void UseConnectionFactory_ThrowsForNullConnectionFactory()
    {
        var builder = new RabbitMqTopologyBuilder();

        Action act = () => builder.UseConnectionFactory((ConnectionFactory) null!);

        act.Should().Throw<ArgumentNullException>().WithParameterName("connectionFactory");
    }

    [Fact]
    public void UseConnectionFactory_ThrowsForNullFactoryFunction()
    {
        var builder = new RabbitMqTopologyBuilder();

        Action act = () => builder.UseConnectionFactory((Func<IServiceProvider, ConnectionFactory>) null!);

        act.Should().Throw<ArgumentNullException>().WithParameterName("createConnectionFactory");
    }

    [Fact]
    public void MapMessageContracts_ThrowsForNullConfigure()
    {
        var builder = new RabbitMqTopologyBuilder();

        Action act = () => builder.MapMessageContracts(null!);

        act.Should().Throw<ArgumentNullException>().WithParameterName("configure");
    }

    [Fact]
    public void Consume_ThrowsForNullConfigure()
    {
        var builder = new RabbitMqTopologyBuilder();

        Action act = () => builder.Consume("queue", null!);

        act.Should().Throw<ArgumentNullException>().WithParameterName("configure");
    }

    [Fact]
    public void ConfigureInboundPipeline_ThrowsForNullConfigure()
    {
        var builder = new RabbitMqTopologyBuilder();

        Action act = () => builder.ConfigureInboundPipeline(null!);

        act.Should().Throw<ArgumentNullException>().WithParameterName("configure");
    }

    [Fact]
    public void Publish_ThrowsForNullConfigure()
    {
        var builder = new RabbitMqTopologyBuilder();

        Action act = () => builder.Publish<ValidationMessageA>(null!);

        act.Should().Throw<ArgumentNullException>().WithParameterName("configure");
    }

    [Fact]
    public void PublishNamed_ThrowsForBlankTargetName()
    {
        var builder = new RabbitMqTopologyBuilder();

        Action act = () => builder.PublishNamed<ValidationMessageA>(" ", _ => { });

        act.Should().Throw<ArgumentException>().WithParameterName("targetName");
    }

    [Fact]
    public void OutboundChannelGroup_ThrowsForZeroMaximumChannelCount()
    {
        var builder = new RabbitMqTopologyBuilder();

        Action act = () => builder.ChannelGroup("group", 0);

        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("maximumChannelCount");
    }

    [Fact]
    public void OutboundChannelGroup_ThrowsForReservedNamePrefix()
    {
        var builder = new RabbitMqTopologyBuilder();

        Action act = () => builder.ChannelGroup("$implicit:custom", 1);

        act.Should().Throw<ArgumentException>().WithParameterName("name");
    }

    [Fact]
    public void InboundChannelGroup_ThrowsForZeroMaximumChannelCount()
    {
        var builder = new RabbitMqTopologyBuilder();

        Action act = () => builder.ChannelGroup("group", 0, 1, 1);

        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("maximumChannelCount");
    }

    [Fact]
    public void InboundChannelGroup_ThrowsForZeroPrefetchCount()
    {
        var builder = new RabbitMqTopologyBuilder();

        Action act = () => builder.ChannelGroup("group", 1, 0, 1);

        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("prefetchCount");
    }

    [Fact]
    public void InboundChannelGroup_ThrowsForZeroConsumerDispatchConcurrency()
    {
        var builder = new RabbitMqTopologyBuilder();

        Action act = () => builder.ChannelGroup("group", 1, 1, 0);

        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("consumerDispatchConcurrency");
    }

    [Fact]
    public void InboundChannelGroup_ThrowsForReservedNamePrefix()
    {
        var builder = new RabbitMqTopologyBuilder();

        Action act = () => builder.ChannelGroup("$implicit:custom", 1, 1, 1);

        act.Should().Throw<ArgumentException>().WithParameterName("name");
    }

    [Fact]
    public void WithShutdownTimeout_ThrowsForZeroOrNegative()
    {
        var builder = new RabbitMqTopologyBuilder();

        Action zero = () => builder.WithShutdownTimeout(TimeSpan.Zero);
        Action negative = () => builder.WithShutdownTimeout(TimeSpan.FromSeconds(-1));

        zero.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("shutdownTimeout");
        negative.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("shutdownTimeout");
    }

    [Fact]
    public void WithDefaultPublisherConfirmMode_ThrowsForInvalidMode()
    {
        var builder = new RabbitMqTopologyBuilder();

        Action act = () => builder.WithDefaultPublisherConfirmMode((RabbitMqPublisherConfirmMode) 999);

        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("publisherConfirmMode");
    }

    [Fact]
    public void WithDefaultPublisherConfirmTimeout_ThrowsForInvalidTimeout()
    {
        var builder = new RabbitMqTopologyBuilder();

        Action act = () => builder.WithDefaultPublisherConfirmTimeout(TimeSpan.Zero);

        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("publisherConfirmTimeout");
    }
}
