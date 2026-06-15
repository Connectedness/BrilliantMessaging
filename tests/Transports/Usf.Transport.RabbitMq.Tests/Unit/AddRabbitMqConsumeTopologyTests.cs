using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using RabbitMQ.Client;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Errors;
using Usf.Core.Messaging.Serialization;
using Usf.Transport.RabbitMq.Tests.TestSupport;
using Xunit;

namespace Usf.Transport.RabbitMq.Tests.Unit;

public sealed class AddRabbitMqConsumeTopologyTests
{
    [Fact]
    public void AddRabbitMqTopology_SupportsOneTopologyWithOutboundTargetsAndInboundEndpoints()
    {
        var services = new ServiceCollection();
        services
           .AddTestCloudEvents()
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Exchange("outbound", ExchangeType.Fanout);
                    builder.Publish<ValidationMessageA>(
                        target => target
                           .ToFanoutExchange("outbound")
                           .WithSerializer<CloudEventMessageSerializer>()
                    );
                    builder.Queue("inbound");
                    builder.Consume(
                        "inbound",
                        endpoint => endpoint.Handle<ValidationMessageA, ValidationMessageAHandler>()
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        serviceProvider
           .GetRequiredService<ITopologyRegistry>()
           .Names.Should().ContainSingle().Which.Should().Be(Topology.DefaultName);
        var topology = serviceProvider.GetRequiredService<Topology>();
        var keyedTopology = serviceProvider.GetRequiredKeyedService<Topology>(Topology.DefaultName);
        var rabbitMqTopology = serviceProvider.GetRequiredKeyedService<RabbitMqTopology>(Topology.DefaultName);

        topology.Should().BeSameAs(rabbitMqTopology);
        keyedTopology.Should().BeSameAs(rabbitMqTopology);
        topology.IsEmpty.Should().BeFalse();
        topology.IsOutboundOnly.Should().BeFalse();
        topology.IsInboundOnly.Should().BeFalse();
        typeof(IDisposable).IsAssignableFrom(typeof(Topology)).Should().BeFalse();
        typeof(IAsyncDisposable).IsAssignableFrom(typeof(Topology)).Should().BeFalse();
        typeof(IDisposable).IsAssignableFrom(typeof(RabbitMqTopology)).Should().BeTrue();
        typeof(IAsyncDisposable).IsAssignableFrom(typeof(RabbitMqTopology)).Should().BeTrue();
        topology.GetRequiredTarget<ValidationMessageA>()
           .TopologyName.Should().Be(Topology.DefaultName);
        topology.InboundEndpoints.Should().ContainSingle()
           .Which.TopologyName.Should().Be(Topology.DefaultName);
    }

    [Fact]
    public void AddRabbitMqTopology_RejectsDuplicateInboundTopologyNames()
    {
        var services = new ServiceCollection();
        var builder = services.AddUsf();
        builder.AddRabbitMqTopology("shared", static _ => { });

        var action = () => builder.AddRabbitMqTopology("shared", static _ => { });

        action
           .Should().Throw<InvalidOperationException>()
           .WithMessage("Topology 'shared' is already registered. Registered topologies: shared.");
    }

    [Fact]
    public void InboundTopologyRegistry_ResolvesNamedTopologiesIndependently()
    {
        const string firstTopologyName = "first";
        const string secondTopologyName = "second";
        var services = new ServiceCollection();
        services
           .AddTestCloudEvents()
           .AddRabbitMqTopology(
                firstTopologyName,
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Queue("first-queue");
                    builder.Consume(
                        "first-queue",
                        endpoint => endpoint.Handle<ValidationMessageA, ValidationMessageAHandler>()
                    );
                }
            )
           .AddRabbitMqTopology(
                secondTopologyName,
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Queue("second-queue");
                    builder.Consume(
                        "second-queue",
                        endpoint => endpoint.Handle<ValidationMessageB, ValidationMessageBHandler>()
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        var registry = serviceProvider.GetRequiredService<ITopologyRegistry>();

        registry.Names.Should().BeEquivalentTo(firstTopologyName, secondTopologyName);
        registry
           .GetRequiredTopology(firstTopologyName)
           .InboundEndpoints.Should().ContainSingle()
           .Which.MessageType.Should().Be(typeof(ValidationMessageA));
        registry
           .GetRequiredTopology(secondTopologyName)
           .InboundEndpoints.Should().ContainSingle()
           .Which.MessageType.Should().Be(typeof(ValidationMessageB));
    }


    [Fact]
    public void AddRabbitMqTopology_CompilesDispatchIndexForInboundAliases()
    {
        var services = new ServiceCollection();
        services
           .AddUsf()
           .UseCloudEvents(options => options.Source = "/tests/rabbitmq")
           .MapMessageContracts(
                contracts => contracts.Map<ValidationMessageA>("tests.current").WithInboundAlias("tests.legacy")
            )
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Queue("inbound");
                    builder.Consume(
                        "inbound",
                        endpoint => endpoint.Handle<ValidationMessageA, ValidationMessageAHandler>()
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        var topology = serviceProvider.GetRequiredService<RabbitMqTopology>();

        topology.TryGetEndpoint("inbound", "tests.current", out var currentEndpoint).Should().BeTrue();
        topology.TryGetEndpoint("inbound", "tests.legacy", out var legacyEndpoint).Should().BeTrue();
        currentEndpoint.Should().BeSameAs(legacyEndpoint);
        currentEndpoint!.Name.Should().Be("inbound:tests.current");
    }

    [Fact]
    public async Task InboundEndpointsForSameMessageType_InvokeTheirDeclaredConcreteHandlers()
    {
        HandlerInvocationSink sink = new ();
        var services = new ServiceCollection();
        services.AddSingleton(sink);
        services.AddTestCloudEvents()
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Queue("first");
                    builder.Queue("second");
                    builder.Consume(
                        "first",
                        endpoint => endpoint.Handle<ValidationMessageA, FirstValidationMessageAHandler>()
                    );
                    builder.Consume(
                        "second",
                        endpoint => endpoint.Handle<ValidationMessageA, SecondValidationMessageAHandler>()
                    );
                }
            );
        await using var serviceProvider = services.BuildServiceProvider();
        var topology = serviceProvider.GetRequiredService<RabbitMqTopology>();
        var firstEndpoint = topology
           .Consumers.Single(static consumer => consumer.QueueName == "first")
           .Endpoints.Single();
        var secondEndpoint = topology
           .Consumers.Single(static consumer => consumer.QueueName == "second")
           .Endpoints.Single();
        using var scope = serviceProvider.CreateScope();
        var message = new ValidationMessageA("value");
        var firstContext = CreateContext(firstEndpoint, scope.ServiceProvider, message);
        var secondContext = CreateContext(secondEndpoint, scope.ServiceProvider, message);

        await firstEndpoint.InvokeHandlerAsync(firstContext);
        await secondEndpoint.InvokeHandlerAsync(secondContext);

        sink.Invocations.Should().Equal("first:value", "second:value");
    }

    [Fact]
    public void AddRabbitMqInboundTopology_AutoRegistersConcreteHandlerAsScoped()
    {
        var services = new ServiceCollection();

        services.AddTestCloudEvents()
           .AddRabbitMqInboundTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Queue("inbound");
                    builder.Consume(
                        "inbound",
                        endpoint => endpoint.Handle<ValidationMessageA, ValidationMessageAHandler>()
                    );
                }
            );

        var descriptor = services.Single(
            static service => service.ServiceType == typeof(ValidationMessageAHandler)
        );
        descriptor.Lifetime.Should().Be(ServiceLifetime.Scoped);
        descriptor.ImplementationType.Should().Be(typeof(ValidationMessageAHandler));
    }

    [Fact]
    public void AddRabbitMqTopology_PreservesExistingConcreteHandlerRegistration()
    {
        var services = new ServiceCollection();
        services.AddSingleton<ValidationMessageAHandler>();

        services.AddTestCloudEvents()
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Queue("inbound");
                    builder.Consume(
                        "inbound",
                        endpoint => endpoint.Handle<ValidationMessageA, ValidationMessageAHandler>()
                    );
                }
            );

        var descriptor = services.Single(
            static service => service.ServiceType == typeof(ValidationMessageAHandler)
        );
        descriptor.Lifetime.Should().Be(ServiceLifetime.Singleton);
    }

    [Fact]
    public void Handle_RejectsInterfaceAndAbstractHandlerTypes()
    {
        var builder = new RabbitMqInboundConsumerBuilder("inbound");

        var interfaceAction = () => builder.Handle<ValidationMessageA, IValidationMessageAHandler>();
        var abstractAction = () => builder.Handle<ValidationMessageA, AbstractValidationMessageAHandler>();

        interfaceAction.Should().Throw<ArgumentException>().WithParameterName("THandler");
        abstractAction.Should().Throw<ArgumentException>().WithParameterName("THandler");
    }

    [Fact]
    public void Compile_RejectsMissingConcreteHandlerService()
    {
        var services = new ServiceCollection();
        services.AddTestCloudEvents()
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Queue("inbound");
                    builder.Consume(
                        "inbound",
                        endpoint => endpoint.Handle<ValidationMessageA, ValidationMessageAHandler>()
                    );
                }
            );
        services.RemoveAll(typeof(ValidationMessageAHandler));
        using var serviceProvider = services.BuildServiceProvider();

        Action action = () => _ = serviceProvider.GetRequiredService<Topology>();

        var exception = action.Should().Throw<TopologyValidationException>().Which;
        exception.ValidationErrors.Should().Contain(
            $"Inbound handler '{typeof(ValidationMessageAHandler)}' for message 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' is not registered."
        );
    }

    [Fact]
    public async Task CreateChannelAsync_RejectsInboundConnectionFactoryWithoutTopologyRecovery()
    {
        var services = new ServiceCollection();
        services.AddTestCloudEvents()
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.UseConnectionFactory(
                        static _ => new ConnectionFactory
                        {
                            AutomaticRecoveryEnabled = true,
                            TopologyRecoveryEnabled = false
                        }
                    );
                    builder.Queue("inbound");
                    builder.Consume(
                        "inbound",
                        endpoint => endpoint.Handle<ValidationMessageA, ValidationMessageAHandler>()
                    );
                }
            );
        await using var serviceProvider = services.BuildServiceProvider();
        var topology = serviceProvider.GetRequiredService<RabbitMqTopology>();

        var action = async () => await topology.CreateChannelAsync(TestContext.Current.CancellationToken);

        var exception = await action.Should().ThrowAsync<TopologyValidationException>();
        exception.Which.ValidationErrors.Should().ContainSingle().Which.Should().Be(
            "RabbitMQ topology recovery must be enabled for inbound topologies so RabbitMQ.Client can recover consumer subscriptions. Configure ConnectionFactory.TopologyRecoveryEnabled to true."
        );
    }

    [Fact]
    public void AddRabbitMqTopology_AppliesCustomInspectorDeserializerAndChannelGroupKnobs()
    {
        var services = new ServiceCollection();
        services.AddSingleton<RawInspector>();
        services.AddSingleton<RawDeserializer>();
        services.AddTestCloudEvents()
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Queue("inbound");
                    builder.ChannelGroup(
                        "shared",
                        maximumChannelCount: 3,
                        prefetchCount: 7,
                        consumerDispatchConcurrency: 2
                    );
                    builder.Consume(
                        "inbound",
                        endpoint => endpoint
                           .UseInspector<RawInspector>()
                           .UseChannelGroup("shared")
                           .Handle<ValidationMessageA, ValidationMessageAHandler>(
                                handler => handler
                                   .WithDeserializer<RawDeserializer>()
                                   .ManualAck()
                            )
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        var topology = serviceProvider.GetRequiredService<RabbitMqTopology>();
        var consumer = topology.Consumers.Should().ContainSingle().Which;
        var endpoint = consumer.Endpoints.Should().ContainSingle().Which;

        consumer.InspectorType.Should().Be(typeof(RawInspector));
        endpoint.DeserializerType.Should().Be(typeof(RawDeserializer));
        endpoint.AckMode.Should().Be(MessageAckMode.Manual);
        consumer.ChannelGroup.Name.Should().Be("shared");
        consumer.ChannelGroup.MaximumChannelCount.Should().Be(3);
        consumer.ChannelGroup.PrefetchCount.Should().Be(7);
        consumer.ChannelGroup.ConsumerDispatchConcurrency.Should().Be(2);
        consumer.CopyBody.Should().BeTrue();
    }

    [Fact]
    public void Compile_RejectsUnregisteredInboundDeserializer()
    {
        var services = new ServiceCollection();
        services.AddTestCloudEvents()
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Queue("inbound");
                    builder.Consume(
                        "inbound",
                        endpoint => endpoint.Handle<ValidationMessageA, ValidationMessageAHandler>(
                            handler => handler.WithDeserializer<RawDeserializer>()
                        )
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        Action action = () => _ = serviceProvider.GetRequiredService<RabbitMqTopology>();

        var exception = action.Should().Throw<TopologyValidationException>().Which;
        exception.ValidationErrors.Should().Contain(
            $"Inbound deserializer '{typeof(RawDeserializer)}' for message 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' is not registered."
        );
    }

    [Fact]
    public void Compile_ConfiguresHandlersOnOneQueueIndependently()
    {
        var services = new ServiceCollection();
        services.AddSingleton<RawDeserializer>();
        services.AddSingleton<AlternateRawDeserializer>();
        services.AddTestCloudEvents()
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Queue("inbound");
                    builder.Consume(
                        "inbound",
                        endpoint => endpoint
                           .Handle<ValidationMessageA, ValidationMessageAHandler>(
                                handler => handler
                                   .WithDeserializer<RawDeserializer>()
                                   .ManualAck()
                            )
                           .Handle<ValidationMessageB, ValidationMessageBHandler>(
                                handler => handler
                                   .WithDeserializer<AlternateRawDeserializer>()
                                   .WithAckMode(MessageAckMode.Auto)
                            )
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        var consumer = serviceProvider.GetRequiredService<RabbitMqTopology>()
           .Consumers.Should().ContainSingle().Which;
        var firstEndpoint = consumer.Endpoints.Single(endpoint => endpoint.MessageType == typeof(ValidationMessageA));
        var secondEndpoint = consumer.Endpoints.Single(endpoint => endpoint.MessageType == typeof(ValidationMessageB));

        firstEndpoint.DeserializerType.Should().Be(typeof(RawDeserializer));
        firstEndpoint.AckMode.Should().Be(MessageAckMode.Manual);
        secondEndpoint.DeserializerType.Should().Be(typeof(AlternateRawDeserializer));
        secondEndpoint.AckMode.Should().Be(MessageAckMode.Auto);
    }

    [Fact]
    public void Compile_UsesDefaultHandlerDeserializerAndAckMode()
    {
        var services = new ServiceCollection();
        services.AddTestCloudEvents()
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Queue("inbound");
                    builder.Consume(
                        "inbound",
                        endpoint => endpoint.Handle<ValidationMessageA, ValidationMessageAHandler>()
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        var endpoint = serviceProvider.GetRequiredService<RabbitMqTopology>()
           .Consumers.Should().ContainSingle().Which
           .Endpoints.Should().ContainSingle().Which;

        endpoint.DeserializerType.Should().Be(typeof(PayloadCodecMessageDeserializer));
        endpoint.AckMode.Should().Be(MessageAckMode.Auto);
    }

    [Fact]
    public void Handle_RejectsUndefinedAckMode()
    {
        var services = new ServiceCollection();

        Action action = () => services.AddTestCloudEvents()
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Queue("inbound");
                    builder.Consume(
                        "inbound",
                        endpoint => endpoint.Handle<ValidationMessageA, ValidationMessageAHandler>(
                            handler => handler.WithAckMode((MessageAckMode) 999)
                        )
                    );
                }
            );

        action.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("ackMode");
    }

    [Fact]
    public void AddRabbitMqTopology_AppliesQueueSettingsRegardlessOfHandleCallOrder()
    {
        var services = new ServiceCollection();
        services.AddTestCloudEvents()
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Queue("inbound");
                    builder.Consume(
                        "inbound",
                        endpoint => endpoint
                           .Handle<ValidationMessageA, ValidationMessageAHandler>()
                           .ZeroCopyBody()
                           .PrefetchCount(7)
                           .Concurrency(3)
                           .ChannelCount(2)
                           .UseInspector<RawInspector>()
                           .Handle<ValidationMessageB, ValidationMessageBHandler>()
                    );
                }
            );
        services.AddSingleton<RawInspector>();
        using var serviceProvider = services.BuildServiceProvider();

        var consumer = serviceProvider.GetRequiredService<RabbitMqTopology>()
           .Consumers.Should().ContainSingle().Which;

        consumer.Endpoints.Should().HaveCount(2);
        consumer.InspectorType.Should().Be(typeof(RawInspector));
        consumer.CopyBody.Should().BeFalse();
        consumer.ChannelGroup.MaximumChannelCount.Should().Be(2);
        consumer.ChannelGroup.PrefetchCount.Should().Be(7);
        consumer.ChannelGroup.ConsumerDispatchConcurrency.Should().Be(3);
    }

    [Fact]
    public void Compile_CoalescesHandlersForOneQueueIntoOneConsumerAndOneImplicitChannelGroup()
    {
        var services = new ServiceCollection();
        services.AddTestCloudEvents()
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Queue("inbound");
                    builder.Consume(
                        "inbound",
                        endpoint => endpoint
                           .Handle<ValidationMessageA, ValidationMessageAHandler>()
                           .Handle<ValidationMessageB, ValidationMessageBHandler>()
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        var topology = serviceProvider.GetRequiredService<RabbitMqTopology>();
        var consumer = topology.Consumers.Should().ContainSingle().Which;

        consumer.Endpoints.Should().HaveCount(2);
        topology.Endpoints.Should().HaveCount(2);
        topology.InboundChannelGroups.Should().ContainSingle().Which.Should().BeSameAs(consumer.ChannelGroup);
        consumer.ChannelGroup.MaximumChannelCount.Should().Be(1);
        consumer.ChannelGroup.PrefetchCount.Should().Be(1);
    }

    [Fact]
    public void Compile_RejectsMultipleConsumeCallsForOneQueue()
    {
        var services = new ServiceCollection();
        services.AddTestCloudEvents()
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Queue("inbound");
                    builder.Consume(
                        "inbound",
                        consumer => consumer.Handle<ValidationMessageA, ValidationMessageAHandler>()
                    );
                    builder.Consume(
                        "inbound",
                        consumer => consumer.Handle<ValidationMessageB, ValidationMessageBHandler>()
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        Action action = () => _ = serviceProvider.GetRequiredService<RabbitMqTopology>();

        action.Should().Throw<TopologyValidationException>()
           .Which.ValidationErrors.Should().Contain("Queue 'inbound' is configured by multiple Consume(...) calls.");
    }

    [Fact]
    public void Compile_RejectsConsumeWithoutHandlers()
    {
        var services = new ServiceCollection();
        services.AddTestCloudEvents()
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Queue("inbound");
                    builder.Consume("inbound", static _ => { });
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        Action action = () => _ = serviceProvider.GetRequiredService<RabbitMqTopology>();

        action
           .Should().Throw<TopologyValidationException>()
           .Which.ValidationErrors.Should().Contain("Consume('inbound') declares no handlers.");
    }

    [Fact]
    public void Compile_ChannelCountCreatesOneConsumerWithMatchingChannelCount()
    {
        var services = new ServiceCollection();
        services.AddTestCloudEvents()
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Queue("inbound");
                    builder.Consume(
                        "inbound",
                        consumer => consumer
                           .ChannelCount(4)
                           .Handle<ValidationMessageA, ValidationMessageAHandler>()
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        var topology = serviceProvider.GetRequiredService<RabbitMqTopology>();

        topology
           .Consumers.Should().ContainSingle()
           .Which.ChannelGroup.MaximumChannelCount.Should().Be(4);
        topology.InboundChannelGroups.Should().ContainSingle();
    }

    [Fact]
    public void SeparatePublishAndConsumeTopologies_OwnDistinctConnectionsAndRegisterRuntimeOnlyForConsumer()
    {
        const string consumerTopologyName = "rabbitmq-consumers";
        var services = new ServiceCollection();
        services
           .AddTestCloudEvents()
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Exchange("orders", ExchangeType.Fanout);
                    builder.Publish<ValidationMessageA>(
                        target => target
                           .ToFanoutExchange("orders")
                           .WithSerializer<CloudEventMessageSerializer>()
                    );
                }
            )
           .AddRabbitMqTopology(
                consumerTopologyName,
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Queue("inbound");
                    builder.Consume(
                        "inbound",
                        endpoint => endpoint.Handle<ValidationMessageA, ValidationMessageAHandler>()
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        var registry = serviceProvider.GetRequiredService<ITopologyRegistry>();
        registry.Names.Should().BeEquivalentTo(Topology.DefaultName, consumerTopologyName);

        // The default topology is the publish topology and has the outbound target.
        registry
           .GetRequiredTopology(Topology.DefaultName)
           .GetRequiredTarget<ValidationMessageA>()
           .TopologyName.Should().Be(Topology.DefaultName);

        // The consuming-only topology is reachable through the registry but exposes no outbound targets.
        var consumerTopology = registry.GetRequiredTopology(consumerTopologyName);
        consumerTopology.OutboundTargets.Should().BeEmpty();
        consumerTopology.InboundEndpoints.Should().ContainSingle()
           .Which.TopologyName.Should().Be(consumerTopologyName);

        // Each topology owns exactly one connection provider, so they are distinct instances.
        var publishTopology = serviceProvider.GetRequiredKeyedService<RabbitMqTopology>(Topology.DefaultName);
        var consumeTopology = serviceProvider.GetRequiredKeyedService<RabbitMqTopology>(consumerTopologyName);
        publishTopology.Should().NotBeSameAs(consumeTopology);

        // A topology runtime is registered only for the consuming topology.
        var runtimes = serviceProvider.GetServices<ITopologyRuntime>();
        runtimes.Should().ContainSingle().Which.TopologyName.Should().Be(consumerTopologyName);
    }

    [Fact]
    public void PublishingThroughConsumingOnlyTopology_FailsWithOutboundTargetNotFound()
    {
        const string consumerTopologyName = "rabbitmq-consumers";
        var services = new ServiceCollection();
        services.AddTestCloudEvents()
           .AddRabbitMqTopology(
                consumerTopologyName,
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Queue("inbound");
                    builder.Consume(
                        "inbound",
                        endpoint => endpoint.Handle<ValidationMessageA, ValidationMessageAHandler>()
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        var topology = serviceProvider
           .GetRequiredService<ITopologyRegistry>()
           .GetRequiredTopology(consumerTopologyName);

        Action action = () => _ = topology.GetRequiredTarget<ValidationMessageA>();

        action.Should().Throw<OutboundTargetNotFoundException>();
    }

    private static IncomingMessageContext CreateContext(
        RabbitMqInboundEndpoint endpoint,
        IServiceProvider services,
        ValidationMessageA message
    )
    {
        return new IncomingMessageContext(
            new TestTransportMessage(),
            endpoint,
            services,
            new NoOpAcknowledgement(),
            TestContext.Current.CancellationToken,
            typeof(ValidationMessageA)
        )
        {
            Message = message
        };
    }

    private sealed class ValidationMessageAHandler : IMessageHandler<ValidationMessageA>
    {
        public Task HandleAsync(
            ValidationMessageA message,
            IncomingMessageContext context,
            CancellationToken cancellationToken
        )
        {
            return Task.CompletedTask;
        }
    }

    private sealed class ValidationMessageBHandler : IMessageHandler<ValidationMessageB>
    {
        public Task HandleAsync(
            ValidationMessageB message,
            IncomingMessageContext context,
            CancellationToken cancellationToken
        )
        {
            return Task.CompletedTask;
        }
    }

    private interface IValidationMessageAHandler : IMessageHandler<ValidationMessageA>;

    private abstract class AbstractValidationMessageAHandler : IMessageHandler<ValidationMessageA>
    {
        public abstract Task HandleAsync(
            ValidationMessageA message,
            IncomingMessageContext context,
            CancellationToken cancellationToken
        );
    }

    private sealed class FirstValidationMessageAHandler : IMessageHandler<ValidationMessageA>
    {
        private readonly HandlerInvocationSink _sink;

        public FirstValidationMessageAHandler(HandlerInvocationSink sink)
        {
            _sink = sink;
        }

        public Task HandleAsync(
            ValidationMessageA message,
            IncomingMessageContext context,
            CancellationToken cancellationToken
        )
        {
            _sink.Invocations.Add($"first:{message.Value}");
            return Task.CompletedTask;
        }
    }

    private sealed class SecondValidationMessageAHandler : IMessageHandler<ValidationMessageA>
    {
        private readonly HandlerInvocationSink _sink;

        public SecondValidationMessageAHandler(HandlerInvocationSink sink)
        {
            _sink = sink;
        }

        public Task HandleAsync(
            ValidationMessageA message,
            IncomingMessageContext context,
            CancellationToken cancellationToken
        )
        {
            _sink.Invocations.Add($"second:{message.Value}");
            return Task.CompletedTask;
        }
    }

    private sealed class HandlerInvocationSink
    {
        public List<string> Invocations { get; } = [];
    }

    private sealed class TestTransportMessage : TransportMessage
    {
        public TestTransportMessage()
            : base("test", "source", ReadOnlyMemory<byte>.Empty, new Dictionary<string, object?>()) { }
    }

    private sealed class NoOpAcknowledgement : IMessageAcknowledgement
    {
        public Task AckAsync(CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }

        public Task NackAsync(bool requeue, CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }
    }

    private sealed class RawInspector : IInboundMessageInspector
    {
        public ValueTask<InboundMessageInspectionResult> InspectAsync(
            TransportMessage transportMessage,
            CancellationToken cancellationToken = default
        )
        {
            return new ValueTask<InboundMessageInspectionResult>(
                new InboundMessageInspectionResult(
                    RabbitMqCloudEventsTestFactory.ValidationMessageADiscriminator,
                    typeof(ValidationMessageA)
                )
            );
        }
    }

    private sealed class RawDeserializer : IMessageDeserializer
    {
        public ValueTask<object?> DeserializeAsync(
            IncomingMessageContext context,
            CancellationToken cancellationToken = default
        )
        {
            return new ValueTask<object?>(new ValidationMessageA("raw"));
        }
    }

    private sealed class AlternateRawDeserializer : IMessageDeserializer
    {
        public ValueTask<object?> DeserializeAsync(
            IncomingMessageContext context,
            CancellationToken cancellationToken = default
        )
        {
            return new ValueTask<object?>(new ValidationMessageB("raw"));
        }
    }
}
