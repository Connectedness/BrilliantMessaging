using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;
using BrilliantMessaging.Transport.Nats.Inbound;
using BrilliantMessaging.Transport.Nats.Tests.TestSupport;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace BrilliantMessaging.Transport.Nats.Tests.Unit;

public sealed class NatsInspectorChainTests
{
    [Fact]
    public async Task DefaultConsumer_CompilesSingleCloudEventsInspector()
    {
        await using var provider = BuildProvider(consumer => consumer.Handle<OrderPlaced, OrderPlacedHandler>());

        var consumer = provider.GetRequiredService<NatsTopology>().Consumers.Should().ContainSingle().Which;

        consumer.InspectorChain.Entries.Should().ContainSingle().Which.Should()
           .BeOfType<CompiledInstanceInboundMessageInspectorChainEntry>().Which.Inspector.Should()
           .BeOfType<CloudEventsInboundMessageInspector>();
    }

    [Fact]
    public async Task UseInspector_ReplacesDefaultChainAndAutoRegistersWithConfiguredLifetime()
    {
        ServiceCollection services = new ();
        await using var provider = BuildProvider(
            services,
            consumer => consumer
               .UseInspector<TestInspector>(ServiceLifetime.Scoped)
               .Handle<OrderPlaced, OrderPlacedHandler>()
        );

        var consumer = provider.GetRequiredService<NatsTopology>().Consumers.Should().ContainSingle().Which;

        consumer.InspectorChain.Entries.Should().ContainSingle().Which.Should()
           .BeOfType<CompiledServiceInboundMessageInspectorChainEntry>().Which.InspectorType.Should()
           .Be(typeof(TestInspector));
        services.Single(descriptor => descriptor.ServiceType == typeof(TestInspector))
           .Lifetime.Should().Be(ServiceLifetime.Scoped);
    }

    [Fact]
    public async Task UseInspectors_KeepsExistingInspectorRegistration()
    {
        ServiceCollection services = new ();
        services.AddSingleton<TestInspector>();
        await using var provider = BuildProvider(
            services,
            consumer => consumer
               .UseInspectors(chain => chain.Use<TestInspector>(ServiceLifetime.Transient))
               .Handle<OrderPlaced, OrderPlacedHandler>()
        );

        provider.GetRequiredService<NatsTopology>().Consumers.Should().ContainSingle();
        services.Single(descriptor => descriptor.ServiceType == typeof(TestInspector))
           .Lifetime.Should().Be(ServiceLifetime.Singleton);
    }

    [Fact]
    public async Task HandlerWithoutContract_IsDispatchedThroughRecognizerDiscriminator()
    {
        await using var provider = BuildProvider(
            consumer => consumer
               .UseInspectors(chain => chain.CloudEvents().When(static _ => true).As<RawEvent>("tests.raw"))
               .Handle<OrderPlaced, OrderPlacedHandler>()
               .Handle<RawEvent, RawEventHandler>()
        );

        var topology = provider.GetRequiredService<NatsTopology>();
        var consumer = topology.Consumers.Should().ContainSingle().Which;

        consumer.EndpointsByDiscriminator.Keys.Should().BeEquivalentTo("tests.order.placed", "tests.raw");
        var endpoint = consumer.EndpointsByDiscriminator["tests.raw"];
        endpoint.MessageType.Should().Be(typeof(RawEvent));
        endpoint.Discriminator.Should().Be("tests.raw");
        endpoint.Name.Should().Be("ORDERS:orders-worker:tests.raw");
        topology.Endpoints.Should().HaveCount(2);
    }

    [Fact]
    public async Task HandlerWithContract_KeepsCanonicalDiscriminatorAndAddsRecognizerDiscriminator()
    {
        await using var provider = BuildProvider(
            consumer => consumer
               .UseInspectors(chain => chain.CloudEvents().WhenHeader("x-legacy").As<OrderPlaced>("tests.legacy"))
               .Handle<OrderPlaced, OrderPlacedHandler>()
        );

        var topology = provider.GetRequiredService<NatsTopology>();
        var consumer = topology.Consumers.Should().ContainSingle().Which;

        consumer.EndpointsByDiscriminator["tests.legacy"].Should()
           .BeSameAs(consumer.EndpointsByDiscriminator["tests.order.placed"]);
        consumer.EndpointsByDiscriminator["tests.legacy"].Discriminator.Should().Be("tests.order.placed");
        topology.Endpoints.Should().ContainSingle();
    }

    [Fact]
    public async Task OutboundOnlyContract_IsValidWhenRecognizerMapsToHandler()
    {
        ServiceCollection services = new ();
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.MapOutbound<OrderPlaced>("tests.outbound"))
           .AddNatsTopology(
                topology => topology
                   .UseServer("nats://localhost:4222")
                   .Stream("ORDERS", stream => stream.Subject("orders.*"))
                   .Consume(
                        "ORDERS",
                        "orders-worker",
                        consumer => consumer
                           .UseInspectors(chain => chain.When(static _ => true).As<OrderPlaced>("tests.raw"))
                           .Handle<OrderPlaced, OrderPlacedHandler>()
                    )
            );
        await using var provider = services.BuildServiceProvider();

        var endpoint = provider.GetRequiredService<NatsTopology>().Endpoints.Should().ContainSingle().Which;

        endpoint.Discriminator.Should().Be("tests.raw");
    }

    [Fact]
    public async Task Recognizer_PrefersExactHandlerOverAssignableHandler()
    {
        await using var provider = BuildProvider(
            consumer => consumer
               .UseInspectors(
                    chain => chain
                       .When(static _ => true).As<DerivedEvent>("tests.derived")
                       .When(static _ => true).As<BaseEvent>("tests.base")
                )
               .Handle<BaseEvent, BaseEventHandler>()
               .Handle<DerivedEvent, DerivedEventHandler>()
        );

        var consumer = provider.GetRequiredService<NatsTopology>().Consumers.Should().ContainSingle().Which;

        consumer.EndpointsByDiscriminator["tests.derived"].MessageType.Should().Be(typeof(DerivedEvent));
        consumer.EndpointsByDiscriminator["tests.base"].MessageType.Should().Be(typeof(BaseEvent));
    }

    [Fact]
    public async Task Compile_RejectsEmptyChain()
    {
        await AssertValidationErrorAsync(
            consumer => consumer
               .UseInspectors(static _ => { })
               .Handle<OrderPlaced, OrderPlacedHandler>(),
            "Inbound inspector chain for NATS consumer 'orders-worker' must contain at least one entry."
        );
    }

    [Fact]
    public async Task Compile_RejectsUnregisteredAsTarget()
    {
        await AssertValidationErrorAsync(
            consumer => consumer
               .UseInspectors(chain => chain.When(static _ => true).As<RawEvent>())
               .Handle<RawEvent, RawEventHandler>(),
            $"Inbound recognizer for message '{typeof(RawEvent)}' on NATS consumer 'orders-worker' uses As<T>() but the type is not registered. Use As<T>(explicitDiscriminator) or register the contract with MessageContractRegistryBuilder.Map<T>(...)."
        );
    }

    [Fact]
    public async Task Compile_RejectsRecognizerWithoutHandler()
    {
        await AssertValidationErrorAsync(
            consumer => consumer
               .UseInspectors(chain => chain.When(static _ => true).As<RawEvent>("tests.raw"))
               .Handle<OrderPlaced, OrderPlacedHandler>(),
            $"Inbound recognizer for message '{typeof(RawEvent)}' maps discriminator 'tests.raw' on NATS consumer 'orders-worker', but no handler on that NATS consumer handles an assignable message type."
        );
    }

    [Fact]
    public async Task Compile_RejectsRecognizerWithSeveralAssignableHandlers()
    {
        await AssertValidationErrorAsync(
            consumer => consumer
               .UseInspectors(chain => chain.When(static _ => true).As<DerivedEvent>("tests.derived"))
               .Handle<BaseEvent, BaseEventHandler>()
               .Handle<IMarkerEvent, MarkerEventHandler>(),
            $"Inbound recognizer for message '{typeof(DerivedEvent)}' maps discriminator 'tests.derived' on NATS consumer 'orders-worker', but multiple handlers on that NATS consumer handle an assignable message type."
        );
    }

    [Fact]
    public async Task Compile_RejectsDiscriminatorMappedToSeveralHandlers()
    {
        await AssertValidationErrorAsync(
            consumer => consumer
               .UseInspectors(chain => chain.When(static _ => true).As<RawEvent>("tests.order.placed"))
               .Handle<OrderPlaced, OrderPlacedHandler>()
               .Handle<RawEvent, RawEventHandler>(),
            "Inbound endpoint discriminator 'tests.order.placed' is configured multiple times for NATS consumer 'orders-worker' on stream 'ORDERS'."
        );
    }

    [Fact]
    public async Task Compile_KeepsContractErrorForHandlerWithoutRecognizer()
    {
        await AssertValidationErrorAsync(
            consumer => consumer
               .UseInspector<TestInspector>()
               .Handle<RawEvent, RawEventHandler>(),
            $"NATS handler '{typeof(RawEventHandler).FullName}' handles message '{typeof(RawEvent).FullName}' which has no registered CloudEvents discriminator."
        );
    }

    [Fact]
    public void UseInspectors_RejectsNullConfigure()
    {
        NatsInboundConsumerBuilder builder = new ("ORDERS", "orders-worker");

        var act = () => builder.UseInspectors(null!);

        act.Should().Throw<ArgumentNullException>().WithParameterName("configure");
    }

    private static async Task AssertValidationErrorAsync(
        Action<NatsInboundConsumerBuilder> configure,
        string expectedError
    )
    {
        await using var provider = BuildProvider(configure);

        // ReSharper disable once AccessToDisposedClosure -- delegate is called before disposal
        var act = () => provider.GetRequiredService<NatsTopology>();

        act.Should().Throw<TopologyValidationException>().Which.ValidationErrors.Should().Contain(expectedError);
    }

    private static ServiceProvider BuildProvider(Action<NatsInboundConsumerBuilder> configure)
    {
        return BuildProvider(new ServiceCollection(), configure);
    }

    private static ServiceProvider BuildProvider(
        ServiceCollection services,
        Action<NatsInboundConsumerBuilder> configure
    )
    {
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology => topology
                   .UseServer("nats://localhost:4222")
                   .Stream("ORDERS", stream => stream.Subject("orders.*"))
                   .Consume("ORDERS", "orders-worker", configure)
            );
        return services.BuildServiceProvider();
    }

    private interface IMarkerEvent;

    private record BaseEvent;

    private sealed record DerivedEvent : BaseEvent, IMarkerEvent;

    private sealed record RawEvent;

    private sealed class TestInspector : IInboundMessageInspector
    {
        public ValueTask<InboundMessageInspectionResult?> InspectAsync(
            TransportMessage transportMessage,
            CancellationToken cancellationToken = default
        )
        {
            return new ValueTask<InboundMessageInspectionResult?>((InboundMessageInspectionResult?) null);
        }
    }

    private sealed class RawEventHandler : NoOpHandler<RawEvent>;

    private sealed class BaseEventHandler : NoOpHandler<BaseEvent>;

    private sealed class DerivedEventHandler : NoOpHandler<DerivedEvent>;

    private sealed class MarkerEventHandler : NoOpHandler<IMarkerEvent>;

    private abstract class NoOpHandler<TMessage> : IMessageHandler<TMessage>
    {
        public Task HandleAsync(
            TMessage message,
            IncomingMessageContext context,
            CancellationToken cancellationToken = default
        )
        {
            return Task.CompletedTask;
        }
    }
}
