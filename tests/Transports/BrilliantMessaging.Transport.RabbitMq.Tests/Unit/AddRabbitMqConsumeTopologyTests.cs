using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;
using BrilliantMessaging.Core.Messaging.Outbound;
using BrilliantMessaging.Transport.RabbitMq.Inbound;
using BrilliantMessaging.Transport.RabbitMq.Tests.TestSupport;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using RabbitMQ.Client;
using Xunit;

namespace BrilliantMessaging.Transport.RabbitMq.Tests.Unit;

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
        var builder = services.AddBrilliantMessaging();
        builder.AddRabbitMqTopology("shared", static _ => { });

        var act = () => builder.AddRabbitMqTopology("shared", static _ => { });

        act.Should().Throw<InvalidOperationException>()
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
           .AddBrilliantMessaging()
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
        currentEndpoint.Name.Should().Be("inbound:tests.current");
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
    public void UseInspectors_RejectsNullConfiguration()
    {
        var builder = new RabbitMqInboundConsumerBuilder("inbound");

        var act = () => builder.UseInspectors(null!);

        act.Should().Throw<ArgumentNullException>().WithParameterName("configure");
    }

    [Fact]
    public void WithRedelivery_RejectsNullConfiguration()
    {
        var consumerBuilder = new RabbitMqInboundConsumerBuilder("inbound");
        RabbitMqInboundHandlerBuilder handlerBuilder = new ();

        var consumerAction = () => consumerBuilder.WithRedelivery(null!);
        var handlerAction = () => handlerBuilder.WithRedelivery(null!);

        consumerAction.Should().Throw<ArgumentNullException>().WithParameterName("configure");
        handlerAction.Should().Throw<ArgumentNullException>().WithParameterName("configure");
    }

    [Fact]
    public void QueueType_RejectsUndefinedQueueType()
    {
        var builder = new RabbitMqInboundConsumerBuilder("inbound");

        var act = () => builder.QueueType((RabbitMqQueueType) 999);

        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("queueType");
    }

    [Fact]
    public void UseInspector_RejectsUnsupportedServiceLifetime()
    {
        var builder = new RabbitMqInboundConsumerBuilder("inbound");

        var act = () => builder.UseInspector<RawInspector>((ServiceLifetime) 999);

        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("serviceLifetime");
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

        // ReSharper disable once AccessToDisposedClosure -- act is called before disposal
        Action act = () => _ = serviceProvider.GetRequiredService<Topology>();

        var exception = act.Should().Throw<TopologyValidationException>().Which;
        exception.ValidationErrors.Should().Contain(
            $"Inbound handler '{typeof(ValidationMessageAHandler)}' for message 'BrilliantMessaging.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' is not registered."
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

        var act = async () => await topology.CreateChannelAsync(TestContext.Current.CancellationToken);

        var exception = await act.Should().ThrowAsync<TopologyValidationException>();
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

        consumer
           .InspectorChain.Entries.Should().ContainSingle().Which.Should()
           .BeOfType<RabbitMqServiceInboundMessageInspectorChainEntry>().Which.InspectorType.Should()
           .Be(typeof(RawInspector));
        endpoint.DeserializerType.Should().Be(typeof(RawDeserializer));
        endpoint.AckMode.Should().Be(MessageAckMode.Manual);
        consumer.ChannelGroup.Name.Should().Be("shared");
        consumer.ChannelGroup.MaximumChannelCount.Should().Be(3);
        consumer.ChannelGroup.PrefetchCount.Should().Be(7);
        consumer.ChannelGroup.ConsumerDispatchConcurrency.Should().Be(2);
        consumer.CopyBody.Should().BeTrue();
    }

    [Fact]
    public void AddRabbitMqTopology_AppliesComposableInspectorChain()
    {
        var services = new ServiceCollection();
        services
           .AddTestCloudEvents()
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Queue("inbound");
                    builder.Consume(
                        "inbound",
                        consumer => consumer
                           .UseInspectors(
                                chain => chain
                                   .CloudEvents()
                                   .WhenHeader("x-legacy-kind", "validation-a").As<ValidationMessageA>()
                            )
                           .Handle<ValidationMessageA, ValidationMessageAHandler>()
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        var consumer = serviceProvider
           .GetRequiredService<RabbitMqTopology>()
           .Consumers.Should().ContainSingle().Which;

        consumer.InspectorChain.Entries.Should().HaveCount(2);
        consumer.InspectorChain.Entries[0].Should()
           .BeOfType<RabbitMqServiceInboundMessageInspectorChainEntry>().Which.InspectorType.Should()
           .Be(typeof(CloudEventsInboundMessageInspector));
        var recognizer = consumer.InspectorChain.Entries[1].Should()
           .BeOfType<RabbitMqInstanceInboundMessageInspectorChainEntry>().Which.Inspector;
        recognizer.Should().BeOfType<PredicateInboundMessageInspector>();
    }

    [Fact]
    public void AddRabbitMqTopology_AutoRegistersCustomInspectorWithConfiguredLifetime()
    {
        var services = new ServiceCollection();
        services
           .AddTestCloudEvents()
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Queue("inbound");
                    builder.Consume(
                        "inbound",
                        consumer => consumer
                           .UseInspectors(chain => chain.Use<RawInspector>(ServiceLifetime.Scoped))
                           .Handle<ValidationMessageA, ValidationMessageAHandler>()
                    );
                }
            );

        services.Should().Contain(
            descriptor => descriptor.ServiceType == typeof(RawInspector) &&
                          descriptor.Lifetime == ServiceLifetime.Scoped
        );
        using var serviceProvider = services.BuildServiceProvider();

        var topology = serviceProvider.GetRequiredService<RabbitMqTopology>();

        topology.Consumers.Should().ContainSingle();
    }

    [Fact]
    public void AddRabbitMqTopology_PreservesExistingInspectorRegistrationOverConfiguredLifetime()
    {
        var services = new ServiceCollection();
        services.AddSingleton<RawInspector>();
        services
           .AddTestCloudEvents()
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Queue("inbound");
                    builder.Consume(
                        "inbound",
                        consumer => consumer
                           .UseInspectors(chain => chain.Use<RawInspector>(ServiceLifetime.Scoped))
                           .Handle<ValidationMessageA, ValidationMessageAHandler>()
                    );
                }
            );

        services
           .Should().ContainSingle(descriptor => descriptor.ServiceType == typeof(RawInspector)).Which
           .Lifetime.Should().Be(ServiceLifetime.Singleton);
        using var serviceProvider = services.BuildServiceProvider();

        serviceProvider.GetRequiredService<RabbitMqTopology>().Consumers.Should().ContainSingle();
    }

    [Fact]
    public void Compile_RejectsEmptyInspectorChain()
    {
        var services = new ServiceCollection();
        services
           .AddTestCloudEvents()
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Queue("inbound");
                    builder.Consume(
                        "inbound",
                        consumer => consumer
                           .UseInspectors(static _ => { })
                           .Handle<ValidationMessageA, ValidationMessageAHandler>()
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        // ReSharper disable once AccessToDisposedClosure -- act is called before disposal
        Action act = () => _ = serviceProvider.GetRequiredService<RabbitMqTopology>();

        act.Should().Throw<TopologyValidationException>()
           .Which.ValidationErrors.Should().ContainSingle().Which.Should().Be(
                "Inbound inspector chain for queue 'inbound' must contain at least one entry."
            );
    }

    [Fact]
    public void Compile_AllowsExplicitRecognizerForMessageTypeWithoutContract()
    {
        var services = new ServiceCollection();
        services
           .AddTestCloudEvents()
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Queue("inbound");
                    builder.Consume(
                        "inbound",
                        consumer => consumer
                           .UseInspectors(
                                chain => chain
                                   .WhenHeader("x-legacy-kind", "upload").As<LegacyUploadMessage>("legacy.upload")
                            )
                           .Handle<LegacyUploadMessage, LegacyUploadMessageHandler>()
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        var topology = serviceProvider.GetRequiredService<RabbitMqTopology>();

        var endpoint = topology.Consumers.Should().ContainSingle().Which.Endpoints.Should().ContainSingle().Which;
        endpoint.Discriminator.Should().Be("legacy.upload");
        topology.TryGetEndpoint("inbound", "legacy.upload", out var selectedEndpoint).Should().BeTrue();
        selectedEndpoint.Should().BeSameAs(endpoint);
    }

    [Fact]
    public void Compile_RejectsRecognizerWithoutAssignableHandlerOnQueue()
    {
        var services = new ServiceCollection();
        services
           .AddTestCloudEvents()
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Queue("inbound");
                    builder.Consume(
                        "inbound",
                        consumer => consumer
                           .UseInspectors(
                                chain => chain
                                   .WhenHeader("x-kind", "validation-b").As<ValidationMessageB>()
                            )
                           .Handle<ValidationMessageA, ValidationMessageAHandler>()
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        // ReSharper disable once AccessToDisposedClosure -- act is called before disposal
        Action act = () => _ = serviceProvider.GetRequiredService<RabbitMqTopology>();

        act.Should().Throw<TopologyValidationException>()
           .Which.ValidationErrors.Should().Contain(
                $"Inbound recognizer for message '{typeof(ValidationMessageB)}' maps discriminator '{RabbitMqCloudEventsTestFactory.ValidationMessageBDiscriminator}' on queue 'inbound', but no handler on that queue handles an assignable message type."
            );
    }

    [Fact]
    public void Compile_RejectsRecognizerAsWithoutRegisteredContract()
    {
        var services = new ServiceCollection();
        services
           .AddTestCloudEvents()
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Queue("inbound");
                    builder.Consume(
                        "inbound",
                        consumer => consumer
                           .UseInspectors(chain => chain.WhenHeader("x-kind").As<LegacyUploadMessage>())
                           .Handle<LegacyUploadMessage, LegacyUploadMessageHandler>()
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        // ReSharper disable once AccessToDisposedClosure -- act is called before disposal
        Action act = () => _ = serviceProvider.GetRequiredService<RabbitMqTopology>();

        act.Should().Throw<TopologyValidationException>()
           .Which.ValidationErrors.Should().Contain(
                $"Inbound recognizer for message '{typeof(LegacyUploadMessage)}' on queue 'inbound' uses As<T>() but the type is not registered. Use As<T>(explicitDiscriminator) or register the contract with MessageContractRegistryBuilder.Map<T>(...)."
            );
    }

    [Fact]
    public void Compile_RejectsRecognizerDiscriminatorCollidingWithAnotherHandler()
    {
        var services = new ServiceCollection();
        services
           .AddTestCloudEvents()
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Queue("inbound");
                    builder.Consume(
                        "inbound",
                        consumer => consumer
                           .UseInspectors(
                                chain => chain
                                   .CloudEvents()
                                   .WhenHeader("x-kind", "collide")
                                   .As<ValidationMessageB>(
                                        RabbitMqCloudEventsTestFactory.ValidationMessageADiscriminator
                                    )
                            )
                           .Handle<ValidationMessageA, ValidationMessageAHandler>()
                           .Handle<ValidationMessageB, ValidationMessageBHandler>()
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        // ReSharper disable once AccessToDisposedClosure -- act is called before disposal
        Action act = () => _ = serviceProvider.GetRequiredService<RabbitMqTopology>();

        act.Should().Throw<TopologyValidationException>()
           .Which.ValidationErrors.Should().Contain(
                $"Inbound endpoint discriminator '{RabbitMqCloudEventsTestFactory.ValidationMessageADiscriminator}' is configured multiple times for queue 'inbound'."
            );
    }

    [Fact]
    public void AddRabbitMqInboundTopology_InterfaceBuilderAppliesSharedBrokerAndPipelineSettings()
    {
        var services = new ServiceCollection();
        services.AddSingleton<RawInspector>();
        services.AddSingleton<RawDeserializer>();
        services.AddSingleton<CustomDeserializationMiddleware>();
        services.AddSingleton<PipelineMarkerMiddleware>();
        services
           .AddTestCloudEvents()
           .AddRabbitMqInboundTopology(
                builder =>
                {
                    builder.UseConnectionFactory(new ConnectionFactory());
                    builder.Exchange("source", ExchangeType.Topic);
                    builder.Exchange("alternate", ExchangeType.Fanout);
                    builder.Queue(
                        "inbound",
                        queue => queue
                           .AsClassicQueue()
                           .DurableQueue(false)
                           .ExclusiveQueue()
                           .AutoDeleteQueue()
                           .WithMessageTtl(TimeSpan.FromSeconds(5))
                           .WithDeadLetterExchange("alternate")
                           .WithDeadLetterRoutingKey("dead")
                    );
                    builder.QueueBinding(
                        "source",
                        "inbound",
                        "validation.*",
                        binding => binding.WithArgument("x-match", "all")
                    );
                    builder.ExchangeBinding("source", "alternate", "overflow");
                    builder.ChannelGroup(
                        "shared-inbound",
                        maximumChannelCount: 2,
                        prefetchCount: 8,
                        consumerDispatchConcurrency: 3
                    );
                    builder.UseDeserializationMiddleware<CustomDeserializationMiddleware>();
                    builder.ConfigureInboundPipeline(pipeline => pipeline.UseMiddleware<PipelineMarkerMiddleware>());
                    builder.WithShutdownTimeout(TimeSpan.FromSeconds(7));
                    builder.Consume(
                        "inbound",
                        endpoint => endpoint
                           .UseInspector<RawInspector>()
                           .UseChannelGroup("shared-inbound")
                           .Handle<ValidationMessageA, ValidationMessageAHandler>(
                                handler => handler.WithDeserializer<RawDeserializer>()
                            )
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        var topology = serviceProvider.GetRequiredKeyedService<RabbitMqTopology>(RabbitMqTopology.DefaultInboundName);
        var queue = topology.Queues.Should().ContainSingle().Which;
        var consumer = topology.Consumers.Should().ContainSingle().Which;

        topology.Name.Should().Be(RabbitMqTopology.DefaultInboundName);
        topology.Exchanges.Should().HaveCount(2);
        topology.Bindings.Should().HaveCount(2);
        topology.ShutdownTimeout.Should().Be(TimeSpan.FromSeconds(7));
        queue.Durable.Should().BeFalse();
        queue.Exclusive.Should().BeTrue();
        queue.AutoDelete.Should().BeTrue();
        queue.Arguments.Should().Contain("x-message-ttl", 5000L);
        queue.Arguments.Should().Contain("x-dead-letter-exchange", "alternate");
        queue.Arguments.Should().Contain("x-dead-letter-routing-key", "dead");
        consumer.ChannelGroup.Name.Should().Be("shared-inbound");
        consumer.ChannelGroup.MaximumChannelCount.Should().Be(2);
        consumer.ChannelGroup.PrefetchCount.Should().Be(8);
        consumer.ChannelGroup.ConsumerDispatchConcurrency.Should().Be(3);
        consumer.InspectorChain.Entries.Should().ContainSingle().Which.Should()
           .BeOfType<RabbitMqServiceInboundMessageInspectorChainEntry>().Which.InspectorType.Should()
           .Be(typeof(RawInspector));
        topology.Pipeline.Should().NotBeNull();
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

        // ReSharper disable once AccessToDisposedClosure -- act is called before disposal
        Action act = () => _ = serviceProvider.GetRequiredService<RabbitMqTopology>();

        var exception = act.Should().Throw<TopologyValidationException>().Which;
        exception.ValidationErrors.Should().Contain(
            $"Inbound deserializer '{typeof(RawDeserializer)}' for message 'BrilliantMessaging.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' is not registered."
        );
    }

    [Fact]
    public void Compile_RejectsUnregisteredInboundDeserializationMiddleware()
    {
        var services = new ServiceCollection();
        services
           .AddTestCloudEvents()
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Queue("inbound");
                    builder.UseDeserializationMiddleware<CustomDeserializationMiddleware>();
                    builder.Consume(
                        "inbound",
                        endpoint => endpoint.Handle<ValidationMessageA, ValidationMessageAHandler>()
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        // ReSharper disable once AccessToDisposedClosure -- act is called before disposal
        Action act = () => _ = serviceProvider.GetRequiredService<RabbitMqTopology>();

        act.Should().Throw<TopologyValidationException>()
           .Which.ValidationErrors.Should().Contain(
                $"Inbound deserialization middleware '{typeof(CustomDeserializationMiddleware)}' is not registered."
            );
    }

    [Fact]
    public void Compile_RejectsOutboundOnlyContractForInboundEndpoint()
    {
        var services = new ServiceCollection();
        services
           .AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests/rabbitmq")
           .MapMessageContracts(contracts => contracts.MapOutbound<ValidationMessageA>("tests.outbound-only"))
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

        // ReSharper disable once AccessToDisposedClosure -- act is called before disposal
        Action act = () => _ = serviceProvider.GetRequiredService<RabbitMqTopology>();

        act.Should().Throw<TopologyValidationException>()
           .Which.ValidationErrors.Should().Contain(
                "Inbound endpoint for message 'BrilliantMessaging.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' has no inbound CloudEvents discriminators. Use MessageContractRegistryBuilder.Map<T>(...) instead of MapOutbound<T>(...)."
            );
    }

    [Fact]
    public void Compile_RejectsDuplicateInboundEndpointNamesAndDispatchKeys()
    {
        var services = new ServiceCollection();
        services
           .AddTestCloudEvents()
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Queue("inbound");
                    builder.Consume(
                        "inbound",
                        endpoint => endpoint
                           .HandleNamed<ValidationMessageA, ValidationMessageAHandler>("duplicate")
                           .HandleNamed<ValidationMessageA, AlternateValidationMessageAHandler>("duplicate")
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        // ReSharper disable once AccessToDisposedClosure -- act is called before disposal
        Action act = () => _ = serviceProvider.GetRequiredService<RabbitMqTopology>();

        var exception = act.Should().Throw<TopologyValidationException>().Which;
        exception.ValidationErrors.Should().Contain("Inbound endpoint name 'duplicate' is configured multiple times.");
        exception.ValidationErrors.Should().Contain(
            "Inbound endpoint discriminator 'tests.rabbitmq.validation-a' is configured multiple times for queue 'inbound'."
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
    public void Compile_UsesQueueTypeResolvedRedeliveryDefaults()
    {
        var services = new ServiceCollection();
        services.AddTestCloudEvents()
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Queue("quorum");
                    builder.Queue("classic", queue => queue.AsClassicQueue());
                    builder.Consume(
                        "quorum",
                        consumer => consumer.Handle<ValidationMessageA, ValidationMessageAHandler>()
                    );
                    builder.Consume(
                        "classic",
                        consumer => consumer.Handle<ValidationMessageB, ValidationMessageBHandler>()
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        var topology = serviceProvider.GetRequiredService<RabbitMqTopology>();
        var quorumEndpoint = topology.Consumers.Single(static consumer => consumer.QueueName == "quorum")
           .Endpoints.Should().ContainSingle().Which;
        var classicEndpoint = topology.Consumers.Single(static consumer => consumer.QueueName == "classic")
           .Endpoints.Should().ContainSingle().Which;

        quorumEndpoint.RedeliveryClassifier.ShouldRetry(new InvalidOperationException()).Should().BeTrue();
        quorumEndpoint.RedeliveryClassifier
           .ShouldRetry(new MessageDeserializationException(typeof(ValidationMessageA), new FormatException()))
           .Should().BeFalse();
        classicEndpoint.RedeliveryClassifier.ShouldRetry(new InvalidOperationException()).Should().BeFalse();
        classicEndpoint.RedeliveryClassifier.ShouldRetry(new RetryMessageException()).Should().BeFalse();
    }

    [Fact]
    public void Compile_ReconcilesConsumerAndHandlerRedeliveryClassifiers()
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
                           .WithRedelivery(
                                redelivery => redelivery.ShouldRetry(static failure => failure is TimeoutException)
                            )
                           .Handle<ValidationMessageA, ValidationMessageAHandler>()
                           .Handle<ValidationMessageB, ValidationMessageBHandler>(
                                handler => handler.WithRedelivery(
                                    redelivery =>
                                        redelivery.ShouldRetry(static failure => failure is ApplicationException)
                                )
                            )
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        var endpoints = serviceProvider.GetRequiredService<RabbitMqTopology>()
           .Consumers.Should().ContainSingle().Which.Endpoints;
        var consumerDefaultEndpoint = endpoints.Single(
            static endpoint => endpoint.MessageType == typeof(ValidationMessageA)
        );
        var handlerOverrideEndpoint = endpoints.Single(
            static endpoint => endpoint.MessageType == typeof(ValidationMessageB)
        );

        consumerDefaultEndpoint.RedeliveryClassifier.ShouldRetry(new TimeoutException()).Should().BeTrue();
        consumerDefaultEndpoint.RedeliveryClassifier.ShouldRetry(new ApplicationException()).Should().BeFalse();
        handlerOverrideEndpoint.RedeliveryClassifier.ShouldRetry(new TimeoutException()).Should().BeFalse();
        handlerOverrideEndpoint.RedeliveryClassifier.ShouldRetry(new ApplicationException()).Should().BeTrue();
    }

    [Fact]
    public void Compile_RejectsExplicitRedeliveryOnClassicOrUnknownQueues()
    {
        var services = new ServiceCollection();
        services.AddTestCloudEvents()
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Queue("classic", queue => queue.AsClassicQueue());
                    builder.Queue("passive", queue => queue.WithDeclareMode(RabbitMqDeclareMode.Passive));
                    builder.Consume(
                        "classic",
                        consumer => consumer
                           .WithRedelivery(static _ => { })
                           .Handle<ValidationMessageA, ValidationMessageAHandler>()
                    );
                    builder.Consume(
                        "passive",
                        consumer => consumer.Handle<ValidationMessageB, ValidationMessageBHandler>(
                            handler => handler.WithRedelivery(static _ => { })
                        )
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        // ReSharper disable once AccessToDisposedClosure -- act is called before disposal
        Action act = () => _ = serviceProvider.GetRequiredService<RabbitMqTopology>();

        var exception = act.Should().Throw<TopologyValidationException>().Which;
        exception.ValidationErrors.Should().Contain(
            "Inbound consumer for queue 'classic' configures redelivery, but the effective queue type is 'classic'. Redelivery classifiers require a quorum queue with a broker delivery limit; call AsQuorumQueue() on the queue declaration or QueueType(RabbitMqQueueType.Quorum) on the consumer."
        );
        exception.ValidationErrors.Should().Contain(
            "Inbound consumer for queue 'passive' configures redelivery, but the effective queue type is 'unknown'. Redelivery classifiers require a quorum queue with a broker delivery limit; call AsQuorumQueue() on the queue declaration or QueueType(RabbitMqQueueType.Quorum) on the consumer."
        );
    }

    [Fact]
    public void Compile_RejectsExplicitRedeliveryOnActiveDefaultQueueType()
    {
        var services = new ServiceCollection();
        services.AddTestCloudEvents()
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Queue("default", queue => queue.UseDefaultQueueType());
                    builder.Consume(
                        "default",
                        consumer => consumer
                           .WithRedelivery(static _ => { })
                           .Handle<ValidationMessageA, ValidationMessageAHandler>()
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        // ReSharper disable once AccessToDisposedClosure -- act is called before disposal
        Action act = () => _ = serviceProvider.GetRequiredService<RabbitMqTopology>();

        act.Should().Throw<TopologyValidationException>()
           .Which.ValidationErrors.Should().Contain(
                "Inbound consumer for queue 'default' configures redelivery, but the effective queue type is 'unknown'. Redelivery classifiers require a quorum queue with a broker delivery limit; call AsQuorumQueue() on the queue declaration or QueueType(RabbitMqQueueType.Quorum) on the consumer."
            );
    }

    [Fact]
    public void Compile_UsesQueueTypeAssertionForPassiveQueueRedelivery()
    {
        var services = new ServiceCollection();
        services.AddTestCloudEvents()
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Queue("passive", queue => queue.WithDeclareMode(RabbitMqDeclareMode.Passive));
                    builder.Consume(
                        "passive",
                        consumer => consumer
                           .QueueType(RabbitMqQueueType.Quorum)
                           .WithRedelivery(static _ => { })
                           .Handle<ValidationMessageA, ValidationMessageAHandler>()
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        var endpoint = serviceProvider.GetRequiredService<RabbitMqTopology>()
           .Consumers.Should().ContainSingle().Which
           .Endpoints.Should().ContainSingle().Which;

        endpoint.RedeliveryClassifier.ShouldRetry(new InvalidOperationException()).Should().BeTrue();
    }

    [Fact]
    public void Compile_RejectsQueueTypeAssertionThatContradictsActiveQueueDeclaration()
    {
        var services = new ServiceCollection();
        services.AddTestCloudEvents()
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Queue("inbound", queue => queue.AsClassicQueue());
                    builder.Consume(
                        "inbound",
                        consumer => consumer
                           .QueueType(RabbitMqQueueType.Quorum)
                           .Handle<ValidationMessageA, ValidationMessageAHandler>()
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        // ReSharper disable once AccessToDisposedClosure -- act is called before disposal
        Action act = () => _ = serviceProvider.GetRequiredService<RabbitMqTopology>();

        act.Should().Throw<TopologyValidationException>()
           .Which.ValidationErrors.Should().ContainSingle().Which.Should().Be(
                "Inbound consumer for queue 'inbound' asserts queue type 'quorum', but active queue declaration 'inbound' sets x-queue-type 'classic'."
            );
    }

    [Fact]
    public void Compile_RejectsClassicOnlyFlagsOnQuorumQueue()
    {
        var services = new ServiceCollection();
        services.AddBrilliantMessaging()
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Queue(
                        "inbound",
                        queue => queue
                           .DurableQueue(false)
                           .ExclusiveQueue()
                           .AutoDeleteQueue()
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        // ReSharper disable once AccessToDisposedClosure -- act is called before disposal
        Action act = () => _ = serviceProvider.GetRequiredService<RabbitMqTopology>();

        act.Should().Throw<TopologyValidationException>()
           .Which.ValidationErrors.Should().Contain(
                "Queue 'inbound' is configured as a quorum queue but sets classic-only flags (durable=false, exclusive=true, autoDelete=true). Quorum queues must be durable, non-exclusive, and non-auto-delete; call AsClassicQueue() to use those flags."
            );
    }

    [Fact]
    public void Handle_RejectsUndefinedAckMode()
    {
        var services = new ServiceCollection();

        var act = () => services
           .AddTestCloudEvents()
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

        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("ackMode");
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
        consumer.InspectorChain.Entries.Should().ContainSingle().Which.Should()
           .BeOfType<RabbitMqServiceInboundMessageInspectorChainEntry>().Which.InspectorType.Should()
           .Be(typeof(RawInspector));
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

        // ReSharper disable once AccessToDisposedClosure -- act is called before disposal
        var act = () => _ = serviceProvider.GetRequiredService<RabbitMqTopology>();

        act.Should().Throw<TopologyValidationException>()
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

        // ReSharper disable once AccessToDisposedClosure -- act is called before disposal
        var act = () => _ = serviceProvider.GetRequiredService<RabbitMqTopology>();

        act
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

        var act = () => _ = topology.GetRequiredTarget<ValidationMessageA>();

        act.Should().Throw<OutboundTargetNotFoundException>();
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

    private sealed class AlternateValidationMessageAHandler : IMessageHandler<ValidationMessageA>
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
        public ValueTask<InboundMessageInspectionResult?> InspectAsync(
            TransportMessage transportMessage,
            CancellationToken cancellationToken = default
        )
        {
            return new ValueTask<InboundMessageInspectionResult?>(
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

    // ReSharper disable once NotAccessedPositionalProperty.Local -- required for serialization
    private sealed record LegacyUploadMessage(string Value);

    private sealed class LegacyUploadMessageHandler : IMessageHandler<LegacyUploadMessage>
    {
        public Task HandleAsync(
            LegacyUploadMessage message,
            IncomingMessageContext context,
            CancellationToken cancellationToken
        )
        {
            return Task.CompletedTask;
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

    private sealed class CustomDeserializationMiddleware : IMessageMiddleware
    {
        public Task InvokeAsync(IncomingMessageContext context, MessageDelegate next)
        {
            return next(context);
        }
    }

    private sealed class PipelineMarkerMiddleware : IMessageMiddleware
    {
        public Task InvokeAsync(IncomingMessageContext context, MessageDelegate next)
        {
            context.Items.SetItem(new MessageContextKey<string>("pipeline-marker"), "seen");
            return next(context);
        }
    }
}
