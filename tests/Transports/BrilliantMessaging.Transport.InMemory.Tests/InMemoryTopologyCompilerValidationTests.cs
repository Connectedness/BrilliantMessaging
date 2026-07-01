using System;
using System.Collections.Immutable;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Outbound;
using BrilliantMessaging.Transport.InMemory.Inbound;
using BrilliantMessaging.Transport.InMemory.Outbound;
using BrilliantMessaging.Transport.InMemory.Tests.TestSupport;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace BrilliantMessaging.Transport.InMemory.Tests;

public sealed class InMemoryTopologyCompilerValidationTests
{
    [Fact]
    public void Compile_RejectsDuplicateDefaultTargetsForSameMessageType()
    {
        var services = CreateServices(static contracts => contracts.Map<OrderPlaced>("tests.order.placed"));
        services.AddBrilliantMessaging().AddInMemoryTopology(
            topology => topology
               .Topic("orders")
               .Publish<OrderPlaced>(target => target.ToTopic("orders"))
               .Publish<OrderPlaced>(target => target.ToTopic("orders"))
        );
        using var serviceProvider = services.BuildServiceProvider();

        var act = () => _ = serviceProvider.GetRequiredService<Topology>();

        act.Should()
           .Throw<TopologyValidationException>()
           .Which
           .ValidationErrors
           .Should()
           .Contain(
                $"Message '{typeof(OrderPlaced).FullName}' configures multiple default in-memory outbound targets."
            );
    }

    [Fact]
    public void Compile_RejectsUnregisteredOutboundMessageContract()
    {
        var services = CreateServices(static contracts => contracts.Map<OrderPlaced>("tests.order.placed"));
        services.AddBrilliantMessaging().AddInMemoryTopology(
            topology => topology
               .Topic("orders")
               .Publish<OrderShipped>(target => target.ToTopic("orders"))
        );
        using var serviceProvider = services.BuildServiceProvider();

        var act = () => _ = serviceProvider.GetRequiredService<Topology>();

        act.Should()
           .Throw<TopologyValidationException>()
           .Which
           .ValidationErrors
           .Should()
           .Contain(
                $"Outbound target for message '{typeof(OrderShipped).FullName}' has no registered CloudEvents discriminator. Register it with MessageContractRegistryBuilder.Map<T>(...)."
            );
    }

    [Fact]
    public void Compile_RejectsConsumerWithoutHandlers()
    {
        var services = CreateServices(static contracts => contracts.Map<OrderPlaced>("tests.order.placed"));
        services.AddBrilliantMessaging().AddInMemoryTopology(
            topology => topology
               .Topic("orders")
               .Consume("orders", static _ => { })
        );
        using var serviceProvider = services.BuildServiceProvider();

        var act = () => _ = serviceProvider.GetRequiredService<Topology>();

        act.Should()
           .Throw<TopologyValidationException>()
           .Which
           .ValidationErrors
           .Should()
           .Contain("Consume('orders') declares no handlers.");
    }

    [Fact]
    public void Compile_RejectsUnregisteredInboundMessageContract()
    {
        var services = CreateServices(static contracts => contracts.Map<OrderPlaced>("tests.order.placed"));
        services.AddBrilliantMessaging().AddInMemoryTopology(
            topology => topology
               .Topic("orders")
               .Consume("orders", consumer => consumer.Handle<OrderShipped, OrderShippedHandler>())
        );
        using var serviceProvider = services.BuildServiceProvider();

        var act = () => _ = serviceProvider.GetRequiredService<Topology>();

        act.Should()
           .Throw<TopologyValidationException>()
           .Which
           .ValidationErrors
           .Should()
           .Contain(
                $"Handler for message '{typeof(OrderShipped).FullName}' on topic 'orders' has no registered CloudEvents discriminator. Register it with MessageContractRegistryBuilder.Map<T>(...)."
            );
    }

    [Fact]
    public void Compile_RejectsConsumerThatDeadLettersToUndeclaredTopic()
    {
        var services = CreateServices(static contracts => contracts.Map<OrderPlaced>("tests.order.placed"));
        services.AddBrilliantMessaging().AddInMemoryTopology(
            topology => topology
               .Topic("orders")
               .Consume(
                    "orders",
                    consumer => consumer
                       .OnFailure(failure => failure.DeadLetterTo("orders.dead"))
                       .Handle<OrderPlaced, OrderPlacedHandler>()
                )
        );
        using var serviceProvider = services.BuildServiceProvider();

        var act = () => _ = serviceProvider.GetRequiredService<Topology>();

        act.Should()
           .Throw<TopologyValidationException>()
           .Which
           .ValidationErrors
           .Should()
           .Contain(
                "Consumer for topic 'orders' dead-letters to undeclared topic 'orders.dead'. Declare it with Topic(\"orders.dead\")."
            );
    }

    [Fact]
    public void Compile_RejectsDuplicateHandlerDiscriminatorOnSingleConsumer()
    {
        var services = CreateServices(static contracts => contracts.Map<OrderPlaced>("tests.order.placed"));
        services.AddBrilliantMessaging().AddInMemoryTopology(
            topology => topology
               .Topic("orders")
               .Consume(
                    "orders",
                    consumer => consumer
                       .Handle<OrderPlaced, OrderPlacedHandler>()
                       .Handle<OrderPlaced, SecondOrderPlacedHandler>()
                )
        );
        using var serviceProvider = services.BuildServiceProvider();

        var act = () => _ = serviceProvider.GetRequiredService<Topology>();

        act.Should()
           .Throw<TopologyValidationException>()
           .Which
           .ValidationErrors
           .Should()
           .Contain(
                "Consumer for topic 'orders' configures more than one handler for CloudEvents type 'tests.order.placed'."
            );
    }

    [Fact]
    public void Compile_AggregatesDirectConfigurationValidationErrors()
    {
        using var serviceProvider = CreateServices(static contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .BuildServiceProvider();
        var compiler = CreateCompiler(serviceProvider);
        var configuration = new InMemoryTopologyConfiguration(
            ImmutableArray.Create("orders"),
            ImmutableArray.Create(
                new InMemoryOutboundTargetDefinition(
                    typeof(OrderPlaced),
                    "orders",
                    "duplicate",
                    typeof(string)
                ),
                new InMemoryOutboundTargetDefinition(
                    typeof(OrderShipped),
                    "missing",
                    "duplicate",
                    null
                )
            ),
            ImmutableArray<InMemoryInboundConsumerDefinition>.Empty,
            InMemoryTopologyBuilder.DefaultShutdownTimeout,
            InMemoryRecordingOptions.Unbounded
        );

        var act = () => compiler.Compile(Topology.DefaultName, configuration);

        var exception = act.Should().Throw<TopologyValidationException>().Which;
        exception.ValidationErrors.Should().Contain(
            "Outbound target name 'duplicate' is configured more than once."
        );
        exception.ValidationErrors.Should().Contain(
            $"Serializer '{typeof(string)}' for the outbound target of message '{typeof(OrderPlaced).FullName}' does not implement '{typeof(IMessageSerializer)}'."
        );
        exception.ValidationErrors.Should().Contain(
            $"Outbound target for message '{typeof(OrderShipped).FullName}' publishes to undeclared topic 'missing'. Declare it with Topic(\"missing\")."
        );
        exception.ValidationErrors.Should().Contain(
            $"Outbound target for message '{typeof(OrderShipped).FullName}' has no registered CloudEvents discriminator. Register it with MessageContractRegistryBuilder.Map<T>(...)."
        );
    }

    [Fact]
    public void Compile_RejectsNullConfiguration()
    {
        using var serviceProvider = CreateServices(static contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .BuildServiceProvider();
        var compiler = CreateCompiler(serviceProvider);

        var act = () => compiler.Compile(Topology.DefaultName, null!);

        act.Should().Throw<ArgumentNullException>().WithParameterName("configuration");
    }

    private static ServiceCollection CreateServices(Action<MessageContractRegistryBuilder> configureContracts)
    {
        ServiceCollection services = new ();
        services
           .AddBrilliantMessaging()
           .UseCloudEvents(static options => options.Source = "/in-memory-compiler-tests")
           .MapMessageContracts(configureContracts);
        return services;
    }

    private static InMemoryTopologyCompiler CreateCompiler(IServiceProvider serviceProvider)
    {
        return new InMemoryTopologyCompiler(
            serviceProvider.GetRequiredService<IMessageContractRegistry>(),
            serviceProvider.GetRequiredService<IMessageSerializer>(),
            serializerType => (IMessageSerializer?) serviceProvider.GetService(serializerType),
            serviceProvider.GetRequiredService<IServiceScopeFactory>(),
            new RealTimeInMemoryDelayScheduler(),
            NullLoggerFactory.Instance
        );
    }
}
