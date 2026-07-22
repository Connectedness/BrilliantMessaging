using System;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Transport.Nats.Tests.TestSupport;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace BrilliantMessaging.Transport.Nats.Tests.Unit;

public sealed class NatsConsumerRedeliveryOrderingTests
{
    [Fact]
    public async Task WithRedeliveryAppliesToHandlersRegisteredBeforeIt()
    {
        ServiceCollection services = new ();
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology => topology
                   .UseServer("nats://localhost:4222")
                   .Stream("ORDERS", stream => stream.Subject("orders.*"))
                   .Consume(
                        "ORDERS",
                        "orders-worker",
                        consumer => consumer
                           .Handle<OrderPlaced, OrderPlacedHandler>()
                           .WithRedelivery(redelivery => redelivery.ShouldRetry(static _ => false))
                    )
            );
        await using var provider = services.BuildServiceProvider();

        var topology = provider.GetRequiredService<NatsTopology>();

        // The consumer-wide classifier is part of the consumer's configuration, not of the handlers registered
        // so far, so it must take effect no matter whether WithRedelivery is chained before or after Handle.
        // RabbitMQ resolves handler ?? consumer ?? default when the topology is compiled, and the same fluent
        // registration must not silently fall back to RetryUnlessPoison on NATS.
        var endpoint = topology.Endpoints.Should().ContainSingle().Which;
        endpoint.RedeliveryClassifier.ShouldRetry(new InvalidOperationException("failure"))
           .Should()
           .BeFalse("the consumer-wide classifier rejects every failure");
    }
}
