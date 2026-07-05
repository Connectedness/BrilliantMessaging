using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;
using BrilliantMessaging.Core.Messaging.Outbound;
using BrilliantMessaging.Transport.Nats.Inbound;
using BrilliantMessaging.Transport.Nats.Tests.TestSupport;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using NATS.Client.Core;
using Xunit;

namespace BrilliantMessaging.Transport.Nats.Tests.Unit;

public sealed class NatsTopologyCompilerTests
{
    [Fact]
    public async Task AddNatsTopology_CompilesTargetsAndConsumers()
    {
        ServiceCollection services = new ();
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology => topology
                   .UseServer("nats://localhost:4222")
                   .Stream("ORDERS", stream => stream.Subject("orders.*").DuplicateWindow(TimeSpan.FromMinutes(2)))
                   .Publish<OrderPlaced>(target => target.ToSubject("orders.placed").UseMessageIdDeduplication())
                   .PublishNamed<OrderPlaced>("audit", target => target.ToSubject("orders.audit"))
                   .Consume(
                        "ORDERS",
                        "orders-worker",
                        consumer => consumer
                           .FilterSubject("orders.placed")
                           .MaxBufferedMessages(16)
                           .DeadLetterSubject("orders.dead")
                           .Handle<OrderPlaced, OrderPlacedHandler>()
                    )
            );

        await using var provider = services.BuildServiceProvider();

        var topology = provider.GetRequiredService<NatsTopology>();

        topology.Streams.Should().ContainSingle().Which.Subjects.Should().Contain("orders.*");
        topology.GetRequiredTarget<OrderPlaced>().Name.Should().Contain("orders.placed");
        topology.GetRequiredTarget<OrderPlaced>("audit").Name.Should().Be("audit");
        var compiledConsumer = topology.Consumers.Should().ContainSingle().Which;
        compiledConsumer.DurableName.Should().Be("orders-worker");
        compiledConsumer.MaxBufferedMessages.Should().Be(16);
        topology.Endpoints.Should().ContainSingle().Which.Discriminator.Should().Be("tests.order.placed");
    }

    [Fact]
    public async Task Compile_RejectsInvalidLiteralPublishSubject()
    {
        ServiceCollection services = new ();
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology => topology
                   .UseServer("nats://localhost:4222")
                   .Stream("ORDERS", stream => stream.Subject("orders.*"))
                   .Publish<OrderPlaced>(target => target.ToSubject("orders.*"))
            );
        await using var provider = services.BuildServiceProvider();

        // ReSharper disable once AccessToDisposedClosure
        var act = () => provider.GetRequiredService<NatsTopology>();

        act.Should().Throw<TopologyValidationException>()
           .Which.ValidationErrors.Should()
           .Contain(error => error.Contains("invalid literal subject", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Compile_RejectsDeadLetterSubjectNotCoveredByStream()
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
                           .FilterSubject("orders.placed")
                           .DeadLetterSubject("dead.orders")
                           .Handle<OrderPlaced, OrderPlacedHandler>()
                    )
            );
        await using var provider = services.BuildServiceProvider();

        // ReSharper disable once AccessToDisposedClosure
        var act = () => provider.GetRequiredService<NatsTopology>();

        act.Should().Throw<TopologyValidationException>()
           .Which.ValidationErrors.Should()
           .Contain(error => error.Contains("Dead-letter subject 'dead.orders'", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Compile_ReportsStreamValidationErrors()
    {
        ServiceCollection services = new ();
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology => topology
                   .UseServer("nats://localhost:4222")
                   .Stream("ORDERS", _ => { })
                   .Stream("ORDERS", stream => stream.Subject("orders..placed"))
            );
        await using var provider = services.BuildServiceProvider();

        // ReSharper disable once AccessToDisposedClosure
        var act = () => provider.GetRequiredService<NatsTopology>();

        act.Should().Throw<TopologyValidationException>()
           .Which.ValidationErrors.Should()
           .Contain(error => error.Contains("configured more than once", StringComparison.Ordinal))
           .And.Contain(error => error.Contains("must declare at least one subject", StringComparison.Ordinal))
           .And.Contain(error => error.Contains("invalid subject pattern 'orders..placed'", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Compile_ReportsOutboundTargetValidationErrors()
    {
        ServiceCollection services = new ();
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology => topology
                   .UseServer("nats://localhost:4222")
                   .Publish<OrderPlaced>(target => target.ToSubject("orders.placed"))
                   .Publish<OrderPlaced>(target => target.ToSubject("orders.audit"))
                   .PublishNamed<OrderPlaced>("audit", target => target.ToSubject("orders.audit"))
                   .PublishNamed<OrderCancelled>("audit", target => target.ToSubject("orders.cancelled"))
                   .Publish<OrderCancelled>(target => target.ToSubject("orders.*"))
            );
        await using var provider = services.BuildServiceProvider();

        // ReSharper disable once AccessToDisposedClosure
        var act = () => provider.GetRequiredService<NatsTopology>();

        act.Should().Throw<TopologyValidationException>()
           .Which.ValidationErrors.Should()
           .Contain(error => error.Contains("multiple default NATS outbound targets", StringComparison.Ordinal))
           .And.Contain(error => error.Contains("Outbound target name 'audit'", StringComparison.Ordinal))
           .And.Contain(error => error.Contains("invalid literal subject 'orders.*'", StringComparison.Ordinal))
           .And.Contain(
                error => error.Contains("has no registered CloudEvents discriminator", StringComparison.Ordinal)
            );
    }

    [Fact]
    public async Task Compile_ReportsConsumerValidationErrors()
    {
        ServiceCollection services = new ();
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology => topology
                   .UseServer("nats://localhost:4222")
                   .Stream("ORDERS", stream => stream.Subject("orders.*"))
                   .Consume("MISSING", "orders-worker", consumer => consumer.Handle<OrderPlaced, OrderPlacedHandler>())
                   .Consume("ORDERS", "orders-worker", consumer => consumer.FilterSubject("orders.*"))
                   .Consume(
                        "ORDERS",
                        "cancelled-worker",
                        consumer => consumer
                           .FilterSubject("orders.cancelled")
                           .DeadLetterSubject("orders.>")
                           .Handle<OrderCancelled, OrderCancelledHandler>()
                    )
            );
        await using var provider = services.BuildServiceProvider();

        // ReSharper disable once AccessToDisposedClosure
        var act = () => provider.GetRequiredService<NatsTopology>();

        act.Should().Throw<TopologyValidationException>()
           .Which.ValidationErrors.Should()
           .Contain(error => error.Contains("configured more than once", StringComparison.Ordinal))
           .And.Contain(error => error.Contains("references missing stream 'MISSING'", StringComparison.Ordinal))
           .And.Contain(error => error.Contains("invalid filter subject 'orders.*'", StringComparison.Ordinal))
           .And.Contain(error => error.Contains("must configure at least one handler", StringComparison.Ordinal))
           .And.Contain(error => error.Contains("invalid dead-letter subject 'orders.>'", StringComparison.Ordinal))
           .And.Contain(
                error => error.Contains("has no registered CloudEvents discriminator", StringComparison.Ordinal)
            );
    }

    [Fact]
    public async Task Compile_RejectsDuplicateHandlersForTheSameMessageType()
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
                           .Handle<OrderPlaced, DuplicateOrderPlacedHandler>()
                    )
            );
        await using var provider = services.BuildServiceProvider();

        // ReSharper disable once AccessToDisposedClosure
        var act = () => provider.GetRequiredService<NatsTopology>();

        act.Should().Throw<TopologyValidationException>()
           .Which.ValidationErrors.Should()
           .Contain(
                error => error.Contains(
                    "configures multiple handlers for message 'BrilliantMessaging.Transport.Nats.Tests.TestSupport.OrderPlaced'",
                    StringComparison.Ordinal
                )
            );
    }

    [Fact]
    public async Task Compile_RejectsUnregisteredDeserializer()
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
                        consumer => consumer.Handle<OrderPlaced, OrderPlacedHandler>(
                            handler => handler.WithDeserializer<RecordingDeserializer>()
                        )
                    )
            );
        await using var provider = services.BuildServiceProvider();

        // ReSharper disable once AccessToDisposedClosure
        var act = () => provider.GetRequiredService<NatsTopology>();

        act.Should().Throw<TopologyValidationException>()
           .Which.ValidationErrors.Should()
           .Contain(
                error => error.Contains(
                    $"Deserializer '{typeof(RecordingDeserializer).FullName}'",
                    StringComparison.Ordinal
                )
            );
    }

    [Fact]
    public async Task Compile_RejectsUnregisteredSerializer()
    {
        ServiceCollection services = new ();
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology => topology
                   .UseServer("nats://localhost:4222")
                   .Publish<OrderPlaced>(
                        target => target.ToSubject("orders.placed").WithSerializer<FixedEnvelopeSerializer>()
                    )
            );
        await using var provider = services.BuildServiceProvider();

        // ReSharper disable once AccessToDisposedClosure
        var act = () => provider.GetRequiredService<NatsTopology>();

        act.Should().Throw<InvalidOperationException>()
           .WithMessage($"Serializer '{typeof(FixedEnvelopeSerializer)}' is not registered.");
    }

    [Fact]
    public async Task Compile_RejectsMissingOptionsAndInvalidMiddleware()
    {
        ServiceCollection services = new ();
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests");
        await using var provider = services.BuildServiceProvider();
        NatsTopologyConfiguration configuration = new (
            null,
            [],
            [],
            [],
            typeof(string),
            null,
            TimeSpan.FromSeconds(5),
            NatsTopologyProvisioningMode.CreateOrUpdate,
            true
        );
        NatsTopologyCompiler compiler = new (
            provider.GetRequiredService<IMessageContractRegistry>(),
            provider.GetRequiredService<IMessageSerializer>(),
            // ReSharper disable once AccessToDisposedClosure
            serializerType => (IMessageSerializer?) provider.GetService(serializerType),
            // ReSharper disable once AccessToDisposedClosure
            serviceType => provider.GetService(serviceType) is not null
        );
        NatsConnectionProvider connectionProvider = new (_ => Task.FromResult(new NatsOpts()));

        var act = () => compiler.Compile("tests", configuration, connectionProvider);

        act.Should().Throw<TopologyValidationException>()
           .Which.ValidationErrors.Should()
           .Contain("NATS connection options must be configured.")
           .And.Contain(error => error.Contains("must implement IMessageMiddleware", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Compile_UsesTopologyLocalMessageContracts()
    {
        ServiceCollection services = new ();
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .AddNatsTopology(
                topology => topology
                   .UseServer("nats://localhost:4222")
                   .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("local.order.placed"))
                   .Stream("ORDERS", stream => stream.Subject("orders.*"))
                   .Publish<OrderPlaced>(target => target.ToSubject("orders.placed"))
                   .Consume(
                        "ORDERS",
                        "orders-worker",
                        consumer => consumer.Handle<OrderPlaced, OrderPlacedHandler>()
                    )
            );
        await using var provider = services.BuildServiceProvider();

        var topology = provider.GetRequiredService<NatsTopology>();

        topology.MessageContractRegistry.GetDiscriminator(typeof(OrderPlaced)).Should().Be("local.order.placed");
        topology.GetRequiredTarget<OrderPlaced>().Name.Should().Contain("orders.placed");
        topology.Endpoints.Should().ContainSingle().Which.Discriminator.Should().Be("local.order.placed");
    }

    [Fact]
    public void Provisioner_MapsJetStreamPolicyKnobs()
    {
        NatsInboundConsumer consumer = new (
            "ORDERS",
            "orders-worker",
            "orders.placed",
            1,
            TimeSpan.FromSeconds(17),
            9,
            42,
            16,
            "orders.dead",
            new Dictionary<string, NatsInboundEndpoint>(StringComparer.Ordinal)
        );

        var config = NatsTopologyProvisioner.ToConsumerConfig(consumer);

        config.DurableName.Should().Be("orders-worker");
        config.FilterSubject.Should().Be("orders.placed");
        config.AckWait.Should().Be(TimeSpan.FromSeconds(17));
        config.MaxDeliver.Should().Be(9);
        config.MaxAckPending.Should().Be(42);
    }

    private sealed class RecordingDeserializer : IMessageDeserializer
    {
        public ValueTask<object?> DeserializeAsync(
            IncomingMessageContext context,
            CancellationToken cancellationToken = default
        )
        {
            return ValueTask.FromResult<object?>(null);
        }
    }

    private sealed class FixedEnvelopeSerializer : IMessageSerializer
    {
        public ValueTask<CloudEventEnvelope> SerializeAsync<T>(
            T message,
            in CloudEventMetadata metadata,
            string? type,
            string? dataSchema,
            CancellationToken cancellationToken = default
        )
        {
            return ValueTask.FromResult(
                new CloudEventEnvelope(
                    "1.0",
                    "message-1",
                    "/tests",
                    type ?? "tests.message",
                    DateTimeOffset.UnixEpoch,
                    null,
                    "application/json",
                    dataSchema,
                    "{}"u8.ToArray()
                )
            );
        }
    }
}
