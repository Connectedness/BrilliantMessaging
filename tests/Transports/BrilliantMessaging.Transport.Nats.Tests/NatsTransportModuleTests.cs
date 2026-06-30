using System;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace BrilliantMessaging.Transport.Nats.Tests;

public sealed class NatsTransportModuleTests
{
    [Fact]
    public async Task AddNatsTopology_DefaultOverloadRegistersDefaultConcreteTopology()
    {
        ServiceCollection services = new ();
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology => topology
                   .UseServer("nats://localhost:4222")
                   .Publish<OrderPlaced>(target => target.ToSubject("orders.placed"))
            );
        await using var provider = services.BuildServiceProvider();

        var topology = provider.GetRequiredService<NatsTopology>();

        topology.Name.Should().Be(Topology.DefaultName);
        topology.Targets.Should().ContainSingle();
        provider.GetServices<ITopologyRuntime>().Should().BeEmpty();
        provider.GetServices<ITopologyProvisioner>().Should().ContainSingle();
    }

    [Fact]
    public async Task AddNatsOutboundTopology_NamedOverloadRegistersPublishOnlyTopology()
    {
        ServiceCollection services = new ();
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsOutboundTopology(
                "nats-out",
                topology => topology
                   .UseServer("nats://localhost:4222")
                   .Publish<OrderPlaced>(target => target.ToSubject("orders.placed"))
            );
        await using var provider = services.BuildServiceProvider();

        var topology = provider.GetRequiredKeyedService<NatsTopology>("nats-out");

        topology.Name.Should().Be("nats-out");
        topology.Targets.Should().ContainSingle();
        provider.GetServices<ITopologyRuntime>().Should().BeEmpty();
    }

    [Fact]
    public async Task AddNatsInboundTopology_DefaultOverloadRegistersInboundRuntime()
    {
        ServiceCollection services = new ();
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsInboundTopology(
                topology => topology
                   .UseServer("nats://localhost:4222")
                   .Stream("ORDERS", stream => stream.Subject("orders.*"))
                   .Consume(
                        "ORDERS",
                        "orders-worker",
                        consumer => consumer.Handle<OrderPlaced, OrderPlacedHandler>()
                    )
            );
        await using var provider = services.BuildServiceProvider();

        var topology = provider.GetRequiredKeyedService<NatsTopology>(NatsTopology.DefaultInboundName);

        topology.Name.Should().Be(NatsTopology.DefaultInboundName);
        topology.Consumers.Should().ContainSingle();
        provider.GetServices<ITopologyRuntime>().Should()
           .ContainSingle(runtime => runtime.TopologyName == topology.Name);
    }

    [Fact]
    public void AddNatsTopology_RejectsNullArguments()
    {
        BrilliantMessagingBuilder builder = null!;
        ServiceCollection services = new ();
        var realBuilder = services.AddBrilliantMessaging();

        Action nullBuilder = () => builder.AddNatsTopology("nats", _ => { });
        Action nullConfigure = () => realBuilder.AddNatsTopology("nats", null!);

        nullBuilder.Should().Throw<ArgumentNullException>().WithParameterName("builder");
        nullConfigure.Should().Throw<ArgumentNullException>().WithParameterName("configure");
    }

    [Fact]
    public void AddNatsOutboundTopology_RejectsNullArguments()
    {
        BrilliantMessagingBuilder builder = null!;
        ServiceCollection services = new ();
        var realBuilder = services.AddBrilliantMessaging();

        Action nullBuilder = () => builder.AddNatsOutboundTopology("nats", _ => { });
        Action nullConfigure = () => realBuilder.AddNatsOutboundTopology("nats", null!);

        nullBuilder.Should().Throw<ArgumentNullException>().WithParameterName("builder");
        nullConfigure.Should().Throw<ArgumentNullException>().WithParameterName("configure");
    }

    [Fact]
    public void AddNatsInboundTopology_RejectsNullArguments()
    {
        BrilliantMessagingBuilder builder = null!;
        ServiceCollection services = new ();
        var realBuilder = services.AddBrilliantMessaging();

        Action nullBuilder = () => builder.AddNatsInboundTopology("nats", _ => { });
        Action nullConfigure = () => realBuilder.AddNatsInboundTopology("nats", null!);

        nullBuilder.Should().Throw<ArgumentNullException>().WithParameterName("builder");
        nullConfigure.Should().Throw<ArgumentNullException>().WithParameterName("configure");
    }
}
