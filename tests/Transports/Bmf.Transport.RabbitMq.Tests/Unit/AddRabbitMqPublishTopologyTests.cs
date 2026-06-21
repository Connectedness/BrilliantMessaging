using System;
using System.Linq;
using System.Threading.Tasks;
using Bmf.Core.Messaging;
using Bmf.Core.Messaging.Inbound;
using Bmf.Core.Messaging.Outbound;
using Bmf.Transport.RabbitMq.Outbound;
using Bmf.Transport.RabbitMq.Tests.TestSupport;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using Xunit;

namespace Bmf.Transport.RabbitMq.Tests.Unit;

public sealed class AddRabbitMqPublishTopologyTests
{
    [Fact]
    public void AddBmf_WiresRegistryPayloadCodecSerializerAndDeserializer()
    {
        var services = new ServiceCollection();
        services
           .AddBmf()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<ValidationMessageA>("tests.validation-a"));
        using var serviceProvider = services.BuildServiceProvider();

        serviceProvider.GetRequiredService<IPayloadCodec>().Should().BeOfType<Utf8JsonPayloadCodec>();
        serviceProvider.GetRequiredService<IMessageSerializer>().Should().BeOfType<CloudEventMessageSerializer>();
        var deserializer = serviceProvider.GetRequiredService<PayloadCodecMessageDeserializer>();
        serviceProvider.GetRequiredService<IMessageDeserializer>().Should().BeSameAs(deserializer);
        serviceProvider
           .GetRequiredService<IMessageContractRegistry>()
           .GetDiscriminator(typeof(ValidationMessageA))
           .Should().Be("tests.validation-a");
    }

    [Fact]
    public void AddBmf_ValidatesSourceWhenOptionsAreResolved()
    {
        var services = new ServiceCollection();
        services.AddBmf().UseCloudEvents(options => options.Source = "   ");
        using var serviceProvider = services.BuildServiceProvider();

        // ReSharper disable once AccessToDisposedClosure -- act is called before disposal
        Action act = () => _ = serviceProvider.GetRequiredService<IOptions<CloudEventsOptions>>().Value;

        act.Should().Throw<OptionsValidationException>();
    }

    [Fact]
    public void Compile_RejectsUnregisteredTypedTargetWhenTopologyIsCompiled()
    {
        var services = new ServiceCollection();
        services
           .AddBmf()
           .UseCloudEvents(options => options.Source = "/tests")
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
            );
        using var serviceProvider = services.BuildServiceProvider();

        // ReSharper disable once AccessToDisposedClosure -- act is called before disposal
        var act = () => _ = serviceProvider.GetRequiredService<Topology>();

        var exception = act.Should().Throw<TopologyValidationException>().Which;
        exception.ValidationErrors.Should().ContainSingle().Which.Should().Be(
            "Outbound target 'Bmf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' publishes unregistered CloudEvents message type 'Bmf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA'. Register its canonical discriminator with MessageContractRegistryBuilder.Map<T>(...) or MapOutbound<T>(...)."
        );
    }

    [Fact]
    public void Compile_AggregatesStructuralAndMessageContractErrorsIntoSingleException()
    {
        var services = new ServiceCollection();
        services
           .AddBmf()
           .UseCloudEvents(options => options.Source = "/tests")
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Exchange("orders", ExchangeType.Fanout);
                    builder.Publish<ValidationMessageA>(
                        target => target
                           .ToFanoutExchange("missing-exchange")
                           .WithSerializer<CloudEventMessageSerializer>()
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        // ReSharper disable once AccessToDisposedClosure -- act is called before disposal
        var act = () => _ = serviceProvider.GetRequiredService<Topology>();

        var exception = act.Should().Throw<TopologyValidationException>().Which;
        exception.ValidationErrors.Should().Contain(
            "Outbound target for message 'Bmf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' references unknown exchange 'missing-exchange'."
        );
        exception.ValidationErrors.Should().Contain(
            "Outbound target 'Bmf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' publishes unregistered CloudEvents message type 'Bmf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA'. Register its canonical discriminator with MessageContractRegistryBuilder.Map<T>(...) or MapOutbound<T>(...)."
        );
    }

    [Fact]
    public void AddRabbitMqTopology_ReportsDeterministicValidationErrors()
    {
        var services = new ServiceCollection();
        services.AddBmf().AddRabbitMqTopology(
            builder =>
            {
                builder.Exchange("exchange-a", ExchangeType.Direct);
                builder.Exchange("exchange-a", ExchangeType.Direct);
                builder.Exchange("internal-a", "internal");
                builder.Queue("queue-a");
                builder.ChannelGroup("shared", 2);
                builder.ChannelGroup("shared", 2);
                builder.QueueBinding(
                    "missing-exchange",
                    "missing-queue",
                    "route-a",
                    binding => binding.WithBindingMode((RabbitMqBindingMode) 99)
                );
                builder.ExchangeBinding("exchange-a", "missing-destination", "route-b");
                builder.Publish<ValidationMessageA>(target => target.ToDirectExchange("missing-exchange", "route-a"));
                builder.Publish<ValidationMessageA>(target => target.ToFanoutExchange("exchange-a"));
                builder.PublishNamed<ValidationMessageA>(
                    "duplicate-target",
                    target => target.ToHeadersExchange("exchange-a").UseChannelGroup("missing-group")
                );
                builder.PublishNamed<ValidationMessageB>(
                    "duplicate-target",
                    target => target.ToTopicExchange("exchange-a", static message => message.Value)
                );
            }
        );
        using var serviceProvider = services.BuildServiceProvider();

        // ReSharper disable once AccessToDisposedClosure -- act is called before disposal
        var act = () => _ = serviceProvider.GetRequiredService<Topology>();

        var exception = act.Should().Throw<TopologyValidationException>().Which;
        exception.ValidationErrors.Should().Equal(
            "A RabbitMQ connection factory must be configured.",
            "Channel group 'shared' is configured but no outbound target references it.",
            "Duplicate channel group 'shared' is configured.",
            "Duplicate exchange 'exchange-a' is configured.",
            "Duplicate target 'duplicate-target' is configured.",
            "Exchange 'internal-a' uses unsupported exchange type 'internal'.",
            "Exchange binding from exchange 'exchange-a' to exchange 'missing-destination' references unknown destination exchange 'missing-destination'.",
            "Message 'Bmf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' configures multiple default RabbitMQ outbound targets.",
            "Outbound target 'Bmf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' publishes unregistered CloudEvents message type 'Bmf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA'. Register its canonical discriminator with MessageContractRegistryBuilder.Map<T>(...) or MapOutbound<T>(...).",
            "Outbound target 'Bmf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' publishes unregistered CloudEvents message type 'Bmf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA'. Register its canonical discriminator with MessageContractRegistryBuilder.Map<T>(...) or MapOutbound<T>(...).",
            "Outbound target 'duplicate-target' publishes unregistered CloudEvents message type 'Bmf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA'. Register its canonical discriminator with MessageContractRegistryBuilder.Map<T>(...) or MapOutbound<T>(...).",
            "Outbound target 'duplicate-target' publishes unregistered CloudEvents message type 'Bmf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageB'. Register its canonical discriminator with MessageContractRegistryBuilder.Map<T>(...) or MapOutbound<T>(...).",
            "Outbound target for message 'Bmf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' and target 'duplicate-target' must configure a serializer.",
            "Outbound target for message 'Bmf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' and target 'duplicate-target' references unknown channel group 'missing-group'.",
            "Outbound target for message 'Bmf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' and target 'duplicate-target' targets exchange 'exchange-a' of type 'direct', but requires 'headers'.",
            "Outbound target for message 'Bmf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' must configure a serializer.",
            "Outbound target for message 'Bmf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' must configure a serializer.",
            "Outbound target for message 'Bmf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' references unknown exchange 'missing-exchange'.",
            "Outbound target for message 'Bmf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' targets exchange 'exchange-a' of type 'direct', but requires 'fanout'.",
            "Outbound target for message 'Bmf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageB' and target 'duplicate-target' must configure a serializer.",
            "Outbound target for message 'Bmf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageB' and target 'duplicate-target' targets exchange 'exchange-a' of type 'direct', but requires 'topic'.",
            "Queue binding from exchange 'missing-exchange' to queue 'missing-queue' references unknown queue 'missing-queue'.",
            "Queue binding from exchange 'missing-exchange' to queue 'missing-queue' references unknown source exchange 'missing-exchange'.",
            "Queue binding from exchange 'missing-exchange' to queue 'missing-queue' uses unsupported binding mode '99'."
        );
    }

    [Fact]
    public void AddRabbitMqTopology_RejectsDuplicateTopologyNames()
    {
        var services = new ServiceCollection();
        var builder = services.AddBmf();
        builder.AddRabbitMqTopology("shared", static _ => { });

        var act = () => builder.AddRabbitMqTopology("shared", static _ => { });

        act.Should().Throw<InvalidOperationException>()
           .WithMessage("Topology 'shared' is already registered. Registered topologies: shared.");
    }

    [Fact]
    public void AddRabbitMqTopology_CompilesDistinctTargetTypesForRabbitMqRoutes()
    {
        var services = new ServiceCollection();
        services.AddTestCloudEvents()
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Exchange("direct-exchange", ExchangeType.Direct);
                    builder.Exchange("topic-exchange", ExchangeType.Topic);
                    builder.Exchange("fanout-exchange", ExchangeType.Fanout);
                    builder.Exchange("headers-exchange", ExchangeType.Headers);
                    builder.Publish<ValidationMessageA>(
                        target => target
                           .ToDirectExchange("direct-exchange", "direct.route")
                           .WithSerializer<CloudEventMessageSerializer>()
                    );
                    builder.PublishNamed<ValidationMessageA>(
                        "topic-target",
                        target => target
                           .ToTopicExchange("topic-exchange", static message => $"topic.{message.Value}")
                           .WithSerializer<CloudEventMessageSerializer>()
                    );
                    builder.PublishNamed<ValidationMessageA>(
                        "fanout-target",
                        target => target
                           .ToFanoutExchange("fanout-exchange")
                           .WithSerializer<CloudEventMessageSerializer>()
                    );
                    builder.PublishNamed<ValidationMessageA>(
                        "headers-target",
                        target => target
                           .ToHeadersExchange("headers-exchange")
                           .WithHeader("region", "eu")
                           .WithSerializer<CloudEventMessageSerializer>()
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        var outboundTopology = serviceProvider.GetRequiredService<Topology>();
        var targetRegistry = serviceProvider.GetRequiredService<Topology>();

        outboundTopology
           .GetRequiredTarget<ValidationMessageA>().GetType()
           .Name.Should().Be("RabbitMqDirectOutboundTarget`1");
        targetRegistry
           .GetRequiredTarget("topic-target").GetType()
           .Name.Should().Be("RabbitMqTopicOutboundTarget`1");
        targetRegistry
           .GetRequiredTarget("fanout-target").GetType()
           .Name.Should().Be("RabbitMqFanoutOutboundTarget`1");
        targetRegistry
           .GetRequiredTarget("headers-target").GetType()
           .Name.Should().Be("RabbitMqHeadersOutboundTarget`1");
    }

    [Fact]
    public void AddRabbitMqOutboundTopology_InterfaceBuilderAppliesBrokerDeclarationsAndPublisherDefaults()
    {
        var services = new ServiceCollection();
        services
           .AddTestCloudEvents()
           .AddRabbitMqOutboundTopology(
                builder =>
                {
                    builder.UseConnectionFactory(new ConnectionFactory());
                    builder.WithDefaultPublisherConfirmMode(RabbitMqPublisherConfirmMode.Confirms);
                    builder.WithDefaultPublisherConfirmTimeout(TimeSpan.FromSeconds(3));
                    builder.Exchange(
                        "orders",
                        ExchangeType.Direct,
                        exchange => exchange
                           .DurableExchange(false)
                           .AutoDeleteExchange()
                           .WithArgument("alternate-exchange", "orders-unrouted")
                    );
                    builder.Exchange(
                        "orders-unrouted",
                        ExchangeType.Fanout,
                        exchange => exchange.WithDeclareMode(RabbitMqDeclareMode.Passive)
                    );
                    builder.Queue(
                        "orders-created",
                        queue => queue
                           .WithDeclareMode(RabbitMqDeclareMode.Passive)
                           .DurableQueue(false)
                           .ExclusiveQueue()
                           .AutoDeleteQueue()
                           .WithArgument("x-custom", "custom")
                           .WithExpires(TimeSpan.FromSeconds(30))
                           .WithMaxLength(100)
                           .WithMaxLengthBytes(4096)
                           .WithQueueType("classic")
                           .SingleActiveConsumer()
                    );
                    builder.QueueBinding(
                        "orders",
                        "orders-created",
                        "orders.created",
                        binding => binding.WithArgument("bind", "queue")
                    );
                    builder.ExchangeBinding(
                        "orders",
                        "orders-unrouted",
                        "orders.unrouted",
                        binding => binding.WithArgument("bind", "exchange")
                    );
                    builder.MapMessageContracts(
                        contracts => contracts.MapOutbound<ValidationMessageA>("tests.rabbitmq.validation-a.outbound")
                    );
                    builder.ChannelGroup(
                        "publishing",
                        maximumChannelCount: 2,
                        publisherConfirmMode: RabbitMqPublisherConfirmMode.Confirms,
                        publisherConfirmTimeout: TimeSpan.FromSeconds(4)
                    );
                    builder.Publish<ValidationMessageA>(
                        target => target
                           .ToDirectExchange("orders", static message => $"orders.{message.Value}")
                           .UseChannelGroup("publishing")
                           .Mandatory()
                           .WithSerializer<CloudEventMessageSerializer>()
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        var topology = serviceProvider.GetRequiredService<RabbitMqTopology>();
        var orders = topology.Exchanges.Single(static exchange => exchange.Name == "orders");
        var queue = topology.Queues.Should().ContainSingle().Which;
        var queueBinding = topology.Bindings.OfType<RabbitMqQueueBindingDefinition>().Should().ContainSingle().Which;
        var exchangeBinding = topology.Bindings.OfType<RabbitMqExchangeBindingDefinition>().Should().ContainSingle().Which;
        var channelGroup = topology.OutboundChannelGroups.Single(static group => group.Name == "publishing");

        orders.Durable.Should().BeFalse();
        orders.AutoDelete.Should().BeTrue();
        orders.Arguments.Should().Contain("alternate-exchange", "orders-unrouted");
        queue.DeclareMode.Should().Be(RabbitMqDeclareMode.Passive);
        queue.Durable.Should().BeFalse();
        queue.Exclusive.Should().BeTrue();
        queue.AutoDelete.Should().BeTrue();
        queue.Arguments.Should().Contain("x-custom", "custom");
        queue.Arguments.Should().Contain("x-expires", 30000L);
        queue.Arguments.Should().Contain("x-max-length", 100L);
        queue.Arguments.Should().Contain("x-max-length-bytes", 4096L);
        queue.Arguments.Should().Contain("x-queue-type", "classic");
        queue.Arguments.Should().Contain("x-single-active-consumer", true);
        queueBinding.Arguments.Should().Contain("bind", "queue");
        exchangeBinding.Arguments.Should().Contain("bind", "exchange");
        channelGroup.MaximumChannelCount.Should().Be(2);
        channelGroup.PublisherConfirmMode.Should().Be(RabbitMqPublisherConfirmMode.Confirms);
        channelGroup.PublisherConfirmTimeout.Should().Be(TimeSpan.FromSeconds(4));
        topology.GetRequiredTarget<ValidationMessageA>()
           .GetRequiredDiscriminator(typeof(ValidationMessageA))
           .Should().Be("tests.rabbitmq.validation-a.outbound");
    }

    [Fact]
    public async Task PublishMessageAsync_ThrowsWhenRoutingKeyIsSuppliedForNonRoutableTarget()
    {
        var services = new ServiceCollection();
        services
           .AddTestCloudEvents()
           .AddRabbitMqOutboundTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Exchange("fanout-exchange", ExchangeType.Fanout);
                    builder.Exchange("headers-exchange", ExchangeType.Headers);
                    builder.PublishNamed<ValidationMessageA>(
                        "fanout-target",
                        target => target
                           .ToFanoutExchange("fanout-exchange")
                           .WithSerializer<CloudEventMessageSerializer>()
                    );
                    builder.PublishNamed<ValidationMessageA>(
                        "headers-target",
                        target => target
                           .ToHeadersExchange("headers-exchange")
                           .WithHeader("region", "eu")
                           .WithSerializer<CloudEventMessageSerializer>()
                    );
                }
            );
        await using var serviceProvider = services.BuildServiceProvider();
        var publisher = serviceProvider.GetRequiredService<IMessagePublisher>();
        var targetRegistry = serviceProvider
           .GetRequiredService<ITopologyRegistry>()
           .GetRequiredTopology(Topology.DefaultName);

        var fanoutPublish = () => publisher.PublishMessageAsync(
            new ValidationMessageA("value"),
            targetRegistry.GetRequiredTarget("fanout-target"),
            routingKey: "explicit.route"
        );
        var headersPublish = () => publisher.PublishMessageAsync(
            new ValidationMessageA("value"),
            targetRegistry.GetRequiredTarget("headers-target"),
            routingKey: "explicit.route"
        );

        (await fanoutPublish.Should().ThrowAsync<OutboundTargetNotRoutableException>())
           .Which.MessageType.Should().Be<ValidationMessageA>();
        await headersPublish.Should().ThrowAsync<OutboundTargetNotRoutableException>();
    }

    [Fact]
    public void GetRequiredRoutingTarget_ThrowsForNonRoutableTarget()
    {
        var services = new ServiceCollection();
        services.AddTestCloudEvents()
           .AddRabbitMqOutboundTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Exchange("fanout-exchange", ExchangeType.Fanout);
                    builder.Publish<ValidationMessageA>(
                        target => target
                           .ToFanoutExchange("fanout-exchange")
                           .WithSerializer<CloudEventMessageSerializer>()
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();
        var outboundTopology = serviceProvider
           .GetRequiredService<ITopologyRegistry>()
           .GetRequiredTopology(Topology.DefaultName);

        var act = () => outboundTopology.GetRequiredRoutingTarget<ValidationMessageA>();

        act.Should().Throw<OutboundTargetNotRoutableException>();
    }

    [Fact]
    public void GetRequiredRoutingTarget_ReturnsRoutableTargetForDirectExchange()
    {
        var services = new ServiceCollection();
        services
           .AddTestCloudEvents()
           .AddRabbitMqOutboundTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Exchange("direct-exchange", ExchangeType.Direct);
                    builder.Publish<ValidationMessageA>(
                        target => target
                           .ToDirectExchange("direct-exchange", "direct.route")
                           .WithSerializer<CloudEventMessageSerializer>()
                    );
                    builder.PublishNamed<ValidationMessageA>(
                        "direct-target",
                        target => target
                           .ToDirectExchange("direct-exchange", "direct.route")
                           .WithSerializer<CloudEventMessageSerializer>()
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();
        var outboundTopology = serviceProvider
           .GetRequiredService<ITopologyRegistry>()
           .GetRequiredTopology(Topology.DefaultName);

        outboundTopology.GetRequiredRoutingTarget<ValidationMessageA>().Should().NotBeNull();
        outboundTopology.GetRequiredRoutingTarget<ValidationMessageA>("direct-target").Should().NotBeNull();
    }

    [Fact]
    public void OutboundTopologyRegistry_ResolvesNamedTopologyTargets()
    {
        const string topologyName = "named";
        var services = new ServiceCollection();
        services.AddTestCloudEvents()
           .AddRabbitMqTopology(
                topologyName,
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
            );
        using var serviceProvider = services.BuildServiceProvider();

        var topology = serviceProvider
           .GetRequiredService<ITopologyRegistry>()
           .GetRequiredTopology(topologyName);

        topology.GetRequiredTarget<ValidationMessageA>().TopologyName.Should().Be(topologyName);
    }

    [Fact]
    public void Compile_RejectsDialectEntryForMessageTypeThatNoTargetPublishes()
    {
        var services = new ServiceCollection();
        services.AddTestCloudEvents()
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.MapMessageContracts(
                        contracts => contracts.MapOutbound<ValidationMessageB>("tests.unused-dialect")
                    );
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Exchange("orders", ExchangeType.Fanout);
                    builder.Publish<ValidationMessageA>(
                        target => target
                           .ToFanoutExchange("orders")
                           .WithSerializer<CloudEventMessageSerializer>()
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        // ReSharper disable once AccessToDisposedClosure -- act is called before disposal
        var act = () => _ = serviceProvider.GetRequiredService<Topology>();

        var exception = act.Should().Throw<TopologyValidationException>().Which;
        exception.ValidationErrors.Should().ContainSingle().Which.Should().Be(
            "RabbitMQ outbound message-contract dialect maps message type 'Bmf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageB', but no outbound target publishes that type on this topology."
        );
    }

    [Fact]
    public void Compile_AllowsDialectOnlyMessageTypeWhenTargetPublishesIt()
    {
        var services = new ServiceCollection();
        services
           .AddBmf()
           .UseCloudEvents(options => options.Source = "/tests/rabbitmq")
           .MapMessageContracts(contracts => contracts.Map<ValidationMessageA>("tests.validation-a"))
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.MapMessageContracts(
                        contracts => contracts.MapOutbound<ValidationMessageB>("tests.dialect-only")
                    );
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.Exchange("orders", ExchangeType.Fanout);
                    builder.Publish<ValidationMessageB>(
                        target => target
                           .ToFanoutExchange("orders")
                           .WithSerializer<CloudEventMessageSerializer>()
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        var target = serviceProvider
           .GetRequiredService<Topology>()
           .GetRequiredTarget<ValidationMessageB>();

        target.GetRequiredDiscriminator(typeof(ValidationMessageB)).Should().Be("tests.dialect-only");
    }

    [Fact]
    public void AddRabbitMqTopology_RejectsMandatoryTargetsUsingFireAndForgetPublishing()
    {
        var services = new ServiceCollection();
        services.AddTestCloudEvents()
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.UseConnectionFactory(static _ => new ConnectionFactory());
                    builder.WithDefaultPublisherConfirmMode(RabbitMqPublisherConfirmMode.FireAndForget);
                    builder.Exchange("orders", ExchangeType.Fanout);
                    builder.ChannelGroup("best-effort", 1, RabbitMqPublisherConfirmMode.FireAndForget);
                    builder.Publish<ValidationMessageA>(
                        target => target
                           .ToFanoutExchange("orders")
                           .Mandatory()
                           .WithSerializer<CloudEventMessageSerializer>()
                    );
                    builder.PublishNamed<ValidationMessageA>(
                        "shared-best-effort",
                        target => target
                           .ToFanoutExchange("orders")
                           .UseChannelGroup("best-effort")
                           .Mandatory()
                           .WithSerializer<CloudEventMessageSerializer>()
                    );
                }
            );
        using var serviceProvider = services.BuildServiceProvider();

        // ReSharper disable once AccessToDisposedClosure -- act is called before disposal
        var act = () => _ = serviceProvider.GetRequiredService<Topology>();

        var exception = act.Should().Throw<TopologyValidationException>().Which;
        exception.ValidationErrors.Should().BeEquivalentTo(
            "Outbound target for message 'Bmf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' enables mandatory routing but its effective channel group uses fire-and-forget publishing.",
            "Outbound target for message 'Bmf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' and target 'shared-best-effort' enables mandatory routing but its effective channel group uses fire-and-forget publishing."
        );
    }
}
