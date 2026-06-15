using System;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Errors;
using Usf.Core.Messaging.Serialization;
using Usf.Transport.RabbitMq.Configuration;
using Usf.Transport.RabbitMq.Tests.TestSupport;
using Xunit;

namespace Usf.Transport.RabbitMq.Tests.Unit;

public sealed class AddRabbitMqPublishTopologyTests
{
    [Fact]
    public void AddUsf_WiresRegistryPayloadCodecSerializerAndDeserializer()
    {
        var services = new ServiceCollection();
        services
           .AddUsf()
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
    public void AddUsf_ValidatesSourceWhenOptionsAreResolved()
    {
        var services = new ServiceCollection();
        services.AddUsf().UseCloudEvents(options => options.Source = "   ");
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
           .AddUsf()
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
        Action action = () => _ = serviceProvider.GetRequiredService<Topology>();

        var exception = action.Should().Throw<TopologyValidationException>().Which;
        exception.ValidationErrors.Should().ContainSingle().Which.Should().Be(
            "Outbound target 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' publishes unregistered CloudEvents message type 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA'. Register its canonical discriminator with MessageContractRegistryBuilder.Map<T>(...) or MapOutbound<T>(...)."
        );
    }

    [Fact]
    public void Compile_AggregatesStructuralAndMessageContractErrorsIntoSingleException()
    {
        var services = new ServiceCollection();
        services
           .AddUsf()
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
        Action action = () => _ = serviceProvider.GetRequiredService<Topology>();

        var exception = action.Should().Throw<TopologyValidationException>().Which;
        exception.ValidationErrors.Should().Contain(
            "Outbound target for message 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' references unknown exchange 'missing-exchange'."
        );
        exception.ValidationErrors.Should().Contain(
            "Outbound target 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' publishes unregistered CloudEvents message type 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA'. Register its canonical discriminator with MessageContractRegistryBuilder.Map<T>(...) or MapOutbound<T>(...)."
        );
    }

    [Fact]
    public void AddRabbitMqTopology_ReportsDeterministicValidationErrors()
    {
        var services = new ServiceCollection();
        services.AddUsf().AddRabbitMqTopology(
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
        Action action = () => _ = serviceProvider.GetRequiredService<Topology>();

        var exception = action.Should().Throw<TopologyValidationException>().Which;
        exception.ValidationErrors.Should().Equal(
            "A RabbitMQ connection factory must be configured.",
            "Channel group 'shared' is configured but no outbound target references it.",
            "Duplicate channel group 'shared' is configured.",
            "Duplicate exchange 'exchange-a' is configured.",
            "Duplicate target 'duplicate-target' is configured.",
            "Exchange 'internal-a' uses unsupported exchange type 'internal'.",
            "Exchange binding from exchange 'exchange-a' to exchange 'missing-destination' references unknown destination exchange 'missing-destination'.",
            "Message 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' configures multiple default RabbitMQ outbound targets.",
            "Outbound target 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' publishes unregistered CloudEvents message type 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA'. Register its canonical discriminator with MessageContractRegistryBuilder.Map<T>(...) or MapOutbound<T>(...).",
            "Outbound target 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' publishes unregistered CloudEvents message type 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA'. Register its canonical discriminator with MessageContractRegistryBuilder.Map<T>(...) or MapOutbound<T>(...).",
            "Outbound target 'duplicate-target' publishes unregistered CloudEvents message type 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA'. Register its canonical discriminator with MessageContractRegistryBuilder.Map<T>(...) or MapOutbound<T>(...).",
            "Outbound target 'duplicate-target' publishes unregistered CloudEvents message type 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageB'. Register its canonical discriminator with MessageContractRegistryBuilder.Map<T>(...) or MapOutbound<T>(...).",
            "Outbound target for message 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' and target 'duplicate-target' must configure a serializer.",
            "Outbound target for message 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' and target 'duplicate-target' references unknown channel group 'missing-group'.",
            "Outbound target for message 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' and target 'duplicate-target' targets exchange 'exchange-a' of type 'direct', but requires 'headers'.",
            "Outbound target for message 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' must configure a serializer.",
            "Outbound target for message 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' must configure a serializer.",
            "Outbound target for message 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' references unknown exchange 'missing-exchange'.",
            "Outbound target for message 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' targets exchange 'exchange-a' of type 'direct', but requires 'fanout'.",
            "Outbound target for message 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageB' and target 'duplicate-target' must configure a serializer.",
            "Outbound target for message 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageB' and target 'duplicate-target' targets exchange 'exchange-a' of type 'direct', but requires 'topic'.",
            "Queue binding from exchange 'missing-exchange' to queue 'missing-queue' references unknown queue 'missing-queue'.",
            "Queue binding from exchange 'missing-exchange' to queue 'missing-queue' references unknown source exchange 'missing-exchange'.",
            "Queue binding from exchange 'missing-exchange' to queue 'missing-queue' uses unsupported binding mode '99'."
        );
    }

    [Fact]
    public void AddRabbitMqTopology_RejectsDuplicateTopologyNames()
    {
        var services = new ServiceCollection();
        var builder = services.AddUsf();
        builder.AddRabbitMqTopology("shared", static _ => { });

        var action = () => builder.AddRabbitMqTopology("shared", static _ => { });

        action.Should().Throw<InvalidOperationException>()
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

        Action action = () => outboundTopology.GetRequiredRoutingTarget<ValidationMessageA>();

        action.Should().Throw<OutboundTargetNotRoutableException>();
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

        Action action = () => _ = serviceProvider.GetRequiredService<Topology>();

        var exception = action.Should().Throw<TopologyValidationException>().Which;
        exception.ValidationErrors.Should().ContainSingle().Which.Should().Be(
            "RabbitMQ outbound message-contract dialect maps message type 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageB', but no outbound target publishes that type on this topology."
        );
    }

    [Fact]
    public void Compile_AllowsDialectOnlyMessageTypeWhenTargetPublishesIt()
    {
        var services = new ServiceCollection();
        services
           .AddUsf()
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
        Action action = () => _ = serviceProvider.GetRequiredService<Topology>();

        var exception = action.Should().Throw<TopologyValidationException>().Which;
        exception.ValidationErrors.Should().BeEquivalentTo(
            "Outbound target for message 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' enables mandatory routing but its effective channel group uses fire-and-forget publishing.",
            "Outbound target for message 'Usf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' and target 'shared-best-effort' enables mandatory routing but its effective channel group uses fire-and-forget publishing."
        );
    }
}
