using System;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Outbound;
using BrilliantMessaging.Transport.Nats.Outbound;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using NATS.Client.Core;
using Xunit;

namespace BrilliantMessaging.Transport.Nats.Tests;

public sealed class NatsOutboundTargetTests
{
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Constructor_MissingSubject_ThrowsArgumentException(string? subject)
    {
        var (serializer, registry) = CreateDependencies();

        var act = () => new NatsOutboundTarget<OrderPlaced>(
            "orders",
            serializer,
            registry,
            topologyName: null!,
            subject!,
            messageIdDeduplication: false,
            CreateConnectionProvider()
        );

        act.Should().Throw<ArgumentException>().WithParameterName("subject");
    }

    [Fact]
    public void Constructor_NullConnectionProvider_ThrowsArgumentNullException()
    {
        var (serializer, registry) = CreateDependencies();

        var act = () => new NatsOutboundTarget<OrderPlaced>(
            "orders",
            serializer,
            registry,
            topologyName: null!,
            "orders.placed",
            messageIdDeduplication: false,
            connectionProvider: null!
        );

        act.Should().Throw<ArgumentNullException>().WithParameterName("connectionProvider");
    }

    private static NatsConnectionProvider CreateConnectionProvider()
    {
        return new NatsConnectionProvider(_ => Task.FromResult(new NatsOpts()));
    }

    private static (IMessageSerializer Serializer, IMessageContractRegistry Registry) CreateDependencies()
    {
        ServiceCollection services = new ();
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"));
        var provider = services.BuildServiceProvider();

        return (
            provider.GetRequiredService<IMessageSerializer>(),
            provider.GetRequiredService<IMessageContractRegistry>()
        );
    }
}
