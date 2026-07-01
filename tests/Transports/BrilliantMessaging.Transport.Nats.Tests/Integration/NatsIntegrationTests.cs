using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;
using BrilliantMessaging.Core.Messaging.Outbound;
using BrilliantMessaging.Transport.Nats.Tests.TestSupport;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Xunit;

namespace BrilliantMessaging.Transport.Nats.Tests.Integration;

[Collection<NatsCollection>]
public sealed class NatsIntegrationTests : IAsyncLifetime
{
    private readonly NatsFixture _fixture;

    public NatsIntegrationTests(NatsFixture fixture)
    {
        _fixture = fixture;
    }

    public async ValueTask InitializeAsync()
    {
        await _fixture.ResetAsync(TestContext.Current.CancellationToken);
    }

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }

    [Fact]
    public async Task PublishConsumeAndProvisionDurableConsumer()
    {
        RecordingProbe probe = new ();
        ServiceCollection services = new ();
        services.AddSingleton(probe);
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology => topology
                   .UseServer(_fixture.ConnectionString)
                   .Stream("ORDERS", stream => stream.Subject("orders.*"))
                   .Publish<OrderPlaced>(target => target.ToSubject("orders.placed"))
                   .Consume(
                        "ORDERS",
                        "orders-worker",
                        consumer => consumer
                           .FilterSubject("orders.placed")
                           .DeadLetterSubject("orders.dead")
                           .Handle<OrderPlaced, RecordingOrderPlacedHandler>()
                    )
            );
        await using var provider = services.BuildServiceProvider();

        foreach (var provisioner in provider.GetServices<ITopologyProvisioner>())
        {
            await provisioner.ProvisionAsync(TestContext.Current.CancellationToken);
        }

        var runtimes = provider.GetServices<ITopologyRuntime>().ToArray();
        foreach (var runtime in runtimes)
        {
            await runtime.StartAsync(TestContext.Current.CancellationToken);
        }

        try
        {
            var publisher = provider.GetRequiredService<IMessagePublisher>();
            await publisher.PublishMessageAsync(
                new OrderPlaced { OrderId = "order-1" },
                cancellationToken: TestContext.Current.CancellationToken
            );

            var received = await probe.WaitAsync(TimeSpan.FromSeconds(15), TestContext.Current.CancellationToken);
            received.OrderId.Should().Be("order-1");
        }
        finally
        {
            foreach (var runtime in runtimes)
            {
                await runtime.StopAsync(CancellationToken.None);
            }
        }
    }

    [Fact]
    public async Task HandlerFailureDeadLettersAndSettlesOriginal()
    {
        RecordingProbe probe = new ();
        ServiceCollection services = new ();
        services.AddSingleton(probe);
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology => topology
                   .UseServer(_fixture.ConnectionString)
                   .Stream("ORDERS", stream => stream.Subject("orders.*"))
                   .Publish<OrderPlaced>(target => target.ToSubject("orders.placed"))
                   .Consume(
                        "ORDERS",
                        "orders-worker",
                        consumer => consumer
                           .FilterSubject("orders.placed")
                           .MaxDeliver(1)
                           .DeadLetterSubject("orders.dead")
                           .Handle<OrderPlaced, FailingOrderPlacedHandler>()
                    )
                   .Consume(
                        "ORDERS",
                        "dead-worker",
                        consumer => consumer
                           .FilterSubject("orders.dead")
                           .Handle<OrderPlaced, RecordingOrderPlacedHandler>()
                    )
            );
        await using var provider = services.BuildServiceProvider();

        foreach (var provisioner in provider.GetServices<ITopologyProvisioner>())
        {
            await provisioner.ProvisionAsync(TestContext.Current.CancellationToken);
        }

        var runtimes = provider.GetServices<ITopologyRuntime>().ToArray();
        foreach (var runtime in runtimes)
        {
            await runtime.StartAsync(TestContext.Current.CancellationToken);
        }

        try
        {
            var publisher = provider.GetRequiredService<IMessagePublisher>();
            await publisher.PublishMessageAsync(
                new OrderPlaced { OrderId = "order-dead" },
                cancellationToken: TestContext.Current.CancellationToken
            );

            var received = await probe.WaitAsync(TimeSpan.FromSeconds(15), TestContext.Current.CancellationToken);
            received.OrderId.Should().Be("order-dead");
        }
        finally
        {
            foreach (var runtime in runtimes)
            {
                await runtime.StopAsync(CancellationToken.None);
            }
        }
    }

    [Fact]
    public async Task RetryableFailureIsRedelivered()
    {
        RedeliveryProbe probe = new ();
        ServiceCollection services = new ();
        services.AddSingleton(probe);
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology => topology
                   .UseServer(_fixture.ConnectionString)
                   .Stream("ORDERS", stream => stream.Subject("orders.*"))
                   .Publish<OrderPlaced>(target => target.ToSubject("orders.placed"))
                   .Consume(
                        "ORDERS",
                        "orders-worker",
                        consumer => consumer
                           .FilterSubject("orders.placed")
                           .MaxDeliver(2)
                           .AckWait(TimeSpan.FromSeconds(2))
                           .Handle<OrderPlaced, RedeliveringOrderPlacedHandler>()
                    )
            );
        await using var provider = services.BuildServiceProvider();

        foreach (var provisioner in provider.GetServices<ITopologyProvisioner>())
        {
            await provisioner.ProvisionAsync(TestContext.Current.CancellationToken);
        }

        var runtimes = provider.GetServices<ITopologyRuntime>().ToArray();
        foreach (var runtime in runtimes)
        {
            await runtime.StartAsync(TestContext.Current.CancellationToken);
        }

        try
        {
            var publisher = provider.GetRequiredService<IMessagePublisher>();
            await publisher.PublishMessageAsync(
                new OrderPlaced { OrderId = "order-retry" },
                cancellationToken: TestContext.Current.CancellationToken
            );

            var attempt = await probe.WaitAsync(TimeSpan.FromSeconds(15), TestContext.Current.CancellationToken);
            attempt.Should().Be(2);
        }
        finally
        {
            foreach (var runtime in runtimes)
            {
                await runtime.StopAsync(CancellationToken.None);
            }
        }
    }

    [Fact]
    public async Task SingleConsumerDispatchesMultipleMessageTypesByCloudEventsDiscriminator()
    {
        MultiMessageProbe probe = new ();
        ServiceCollection services = new ();
        services.AddSingleton(probe);
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(
                contracts =>
                {
                    contracts.Map<OrderPlaced>("tests.order.placed");
                    contracts.Map<OrderCancelled>("tests.order.cancelled");
                }
            )
           .AddNatsTopology(
                topology => topology
                   .UseServer(_fixture.ConnectionString)
                   .Stream("ORDERS", stream => stream.Subject("orders.*"))
                   .Publish<OrderPlaced>(target => target.ToSubject("orders.multi"))
                   .Publish<OrderCancelled>(target => target.ToSubject("orders.multi"))
                   .Consume(
                        "ORDERS",
                        "orders-multi-worker",
                        consumer => consumer
                           .FilterSubject("orders.multi")
                           .Handle<OrderPlaced, MultiOrderPlacedHandler>()
                           .Handle<OrderCancelled, MultiOrderCancelledHandler>()
                    )
            );
        await using var provider = services.BuildServiceProvider();

        foreach (var provisioner in provider.GetServices<ITopologyProvisioner>())
        {
            await provisioner.ProvisionAsync(TestContext.Current.CancellationToken);
        }

        var runtimes = provider.GetServices<ITopologyRuntime>().ToArray();
        foreach (var runtime in runtimes)
        {
            await runtime.StartAsync(TestContext.Current.CancellationToken);
        }

        try
        {
            var publisher = provider.GetRequiredService<IMessagePublisher>();
            await publisher.PublishMessageAsync(
                new OrderPlaced { OrderId = "order-multi-placed" },
                cancellationToken: TestContext.Current.CancellationToken
            );
            await publisher.PublishMessageAsync(
                new OrderCancelled { OrderId = "order-multi-cancelled" },
                cancellationToken: TestContext.Current.CancellationToken
            );

            var placed = await probe.WaitForPlacedAsync(
                TimeSpan.FromSeconds(15),
                TestContext.Current.CancellationToken
            );
            var cancelled = await probe.WaitForCancelledAsync(
                TimeSpan.FromSeconds(15),
                TestContext.Current.CancellationToken
            );
            placed.OrderId.Should().Be("order-multi-placed");
            cancelled.OrderId.Should().Be("order-multi-cancelled");
        }
        finally
        {
            foreach (var runtime in runtimes)
            {
                await runtime.StopAsync(CancellationToken.None);
            }
        }
    }

    [Fact]
    public async Task AckProgressKeepsLongRunningHandlerInFlight()
    {
        SlowHandlerProbe probe = new ();
        ServiceCollection services = new ();
        services.AddSingleton(probe);
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology => topology
                   .UseServer(_fixture.ConnectionString)
                   .Stream("ORDERS", stream => stream.Subject("orders.*"))
                   .Publish<OrderPlaced>(target => target.ToSubject("orders.slow"))
                   .Consume(
                        "ORDERS",
                        "orders-slow-worker",
                        consumer => consumer
                           .FilterSubject("orders.slow")
                           .Concurrency(2)
                           .MaxDeliver(2)
                           .AckWait(TimeSpan.FromSeconds(2))
                           .Handle<OrderPlaced, SlowOrderPlacedHandler>()
                    )
            );
        await using var provider = services.BuildServiceProvider();

        foreach (var provisioner in provider.GetServices<ITopologyProvisioner>())
        {
            await provisioner.ProvisionAsync(TestContext.Current.CancellationToken);
        }

        var runtimes = provider.GetServices<ITopologyRuntime>().ToArray();
        foreach (var runtime in runtimes)
        {
            await runtime.StartAsync(TestContext.Current.CancellationToken);
        }

        try
        {
            var publisher = provider.GetRequiredService<IMessagePublisher>();
            await publisher.PublishMessageAsync(
                new OrderPlaced { OrderId = "order-slow" },
                cancellationToken: TestContext.Current.CancellationToken
            );

            var attempt = await probe.WaitForCompletionAsync(
                TimeSpan.FromSeconds(15),
                TestContext.Current.CancellationToken
            );
            attempt.Should().Be(1);

            await Task.Delay(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken);
            probe.InvocationCount.Should().Be(1);
        }
        finally
        {
            foreach (var runtime in runtimes)
            {
                await runtime.StopAsync(CancellationToken.None);
            }
        }
    }

    [Fact]
    public async Task DisabledAckProgressAllowsLongRunningHandlerToBeRedelivered()
    {
        NoProgressProbe probe = new ();
        ServiceCollection services = new ();
        services.AddSingleton(probe);
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology => topology
                   .UseServer(_fixture.ConnectionString)
                   .AckProgress(false)
                   .Stream("ORDERS", stream => stream.Subject("orders.*"))
                   .Publish<OrderPlaced>(target => target.ToSubject("orders.no-progress"))
                   .Consume(
                        "ORDERS",
                        "orders-no-progress-worker",
                        consumer => consumer
                           .FilterSubject("orders.no-progress")
                           .Concurrency(2)
                           .MaxDeliver(2)
                           .AckWait(TimeSpan.FromSeconds(1))
                           .Handle<OrderPlaced, NoProgressOrderPlacedHandler>()
                    )
            );
        await using var provider = services.BuildServiceProvider();

        foreach (var provisioner in provider.GetServices<ITopologyProvisioner>())
        {
            await provisioner.ProvisionAsync(TestContext.Current.CancellationToken);
        }

        var runtimes = provider.GetServices<ITopologyRuntime>().ToArray();
        foreach (var runtime in runtimes)
        {
            await runtime.StartAsync(TestContext.Current.CancellationToken);
        }

        try
        {
            var publisher = provider.GetRequiredService<IMessagePublisher>();
            await publisher.PublishMessageAsync(
                new OrderPlaced { OrderId = "order-no-progress" },
                cancellationToken: TestContext.Current.CancellationToken
            );

            var attempt = await probe.WaitForRedeliveryAsync(
                TimeSpan.FromSeconds(15),
                TestContext.Current.CancellationToken
            );
            attempt.Should().Be(2);
            probe.InvocationCount.Should().BeGreaterThanOrEqualTo(2);
        }
        finally
        {
            foreach (var runtime in runtimes)
            {
                await runtime.StopAsync(CancellationToken.None);
            }
        }
    }

    [Fact]
    public async Task ManualAcknowledgementSettlesJetStreamMessage()
    {
        ManualAckProbe probe = new ();
        ServiceCollection services = new ();
        services.AddSingleton(probe);
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology => topology
                   .UseServer(_fixture.ConnectionString)
                   .Stream("ORDERS", stream => stream.Subject("orders.*"))
                   .Publish<OrderPlaced>(target => target.ToSubject("orders.manual"))
                   .Consume(
                        "ORDERS",
                        "orders-manual-worker",
                        consumer => consumer
                           .FilterSubject("orders.manual")
                           .Concurrency(2)
                           .MaxDeliver(2)
                           .AckWait(TimeSpan.FromSeconds(1))
                           .Handle<OrderPlaced, ManualAckOrderPlacedHandler>(handler => handler.ManualAck())
                    )
            );
        await using var provider = services.BuildServiceProvider();

        foreach (var provisioner in provider.GetServices<ITopologyProvisioner>())
        {
            await provisioner.ProvisionAsync(TestContext.Current.CancellationToken);
        }

        var runtimes = provider.GetServices<ITopologyRuntime>().ToArray();
        foreach (var runtime in runtimes)
        {
            await runtime.StartAsync(TestContext.Current.CancellationToken);
        }

        try
        {
            var publisher = provider.GetRequiredService<IMessagePublisher>();
            await publisher.PublishMessageAsync(
                new OrderPlaced { OrderId = "order-manual" },
                cancellationToken: TestContext.Current.CancellationToken
            );

            var attempt = await probe.WaitAsync(TimeSpan.FromSeconds(15), TestContext.Current.CancellationToken);
            attempt.Should().Be(1);

            await Task.Delay(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken);
            probe.InvocationCount.Should().Be(1);
        }
        finally
        {
            foreach (var runtime in runtimes)
            {
                await runtime.StopAsync(CancellationToken.None);
            }
        }
    }

    [Fact]
    public async Task ManualAcknowledgementWithoutSettlementIsRedelivered()
    {
        ManualNoAckProbe probe = new ();
        ServiceCollection services = new ();
        services.AddSingleton(probe);
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology => topology
                   .UseServer(_fixture.ConnectionString)
                   .Stream("ORDERS", stream => stream.Subject("orders.*"))
                   .Publish<OrderPlaced>(target => target.ToSubject("orders.unsettled"))
                   .Consume(
                        "ORDERS",
                        "orders-manual-unsettled-worker",
                        consumer => consumer
                           .FilterSubject("orders.unsettled")
                           .MaxDeliver(2)
                           .AckWait(TimeSpan.FromSeconds(1))
                           .Handle<OrderPlaced, ManualNoAckOrderPlacedHandler>(handler => handler.ManualAck())
                    )
            );
        await using var provider = services.BuildServiceProvider();

        foreach (var provisioner in provider.GetServices<ITopologyProvisioner>())
        {
            await provisioner.ProvisionAsync(TestContext.Current.CancellationToken);
        }

        var runtimes = provider.GetServices<ITopologyRuntime>().ToArray();
        foreach (var runtime in runtimes)
        {
            await runtime.StartAsync(TestContext.Current.CancellationToken);
        }

        try
        {
            var publisher = provider.GetRequiredService<IMessagePublisher>();
            await publisher.PublishMessageAsync(
                new OrderPlaced { OrderId = "order-manual-unsettled" },
                cancellationToken: TestContext.Current.CancellationToken
            );

            var attempt = await probe.WaitForRedeliveryAsync(
                TimeSpan.FromSeconds(15),
                TestContext.Current.CancellationToken
            );
            attempt.Should().Be(2);
            probe.InvocationCount.Should().BeGreaterThanOrEqualTo(2);
        }
        finally
        {
            foreach (var runtime in runtimes)
            {
                await runtime.StopAsync(CancellationToken.None);
            }
        }
    }

    [Fact]
    public async Task SerializedPublishMapsCloudEventHeadersForNatsWire()
    {
        ServiceCollection services = new ();
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology => topology
                   .UseServer(_fixture.ConnectionString)
                   .Stream("ORDERS", stream => stream.Subject("orders.*"))
                   .Publish<OrderPlaced>(target => target.ToSubject("orders.raw").UseMessageIdDeduplication())
            );
        await using var provider = services.BuildServiceProvider();

        foreach (var provisioner in provider.GetServices<ITopologyProvisioner>())
        {
            await provisioner.ProvisionAsync(TestContext.Current.CancellationToken);
        }

        await using NatsConnection connection = new (new NatsOpts { Url = _fixture.ConnectionString });
        NatsJSContext jetStream = new (connection);
        await jetStream.CreateOrUpdateConsumerAsync(
            "ORDERS",
            new ConsumerConfig("wire-inspector")
            {
                DurableName = "wire-inspector",
                FilterSubject = "orders.raw",
                AckPolicy = ConsumerConfigAckPolicy.Explicit,
                DeliverPolicy = ConsumerConfigDeliverPolicy.All
            },
            TestContext.Current.CancellationToken
        );

        var target = provider.GetRequiredService<NatsTopology>().GetRequiredTarget<OrderPlaced>();
        SerializedMessage message = new (
            "body"u8.ToArray(),
            "application/json",
            "gzip",
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                ["cloudEvents:type"] = "tests.order.placed",
                ["traceparent"] = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00"
            },
            "message-raw",
            null
        );

        await target.PublishSerializedAsync(message, TestContext.Current.CancellationToken);

        var received = await ReadSingleMessageAsync(
            jetStream,
            "ORDERS",
            "wire-inspector",
            TestContext.Current.CancellationToken
        );
        received.Subject.Should().Be("orders.raw");
        received.Data.Should().Equal("body"u8.ToArray());
        var headers = GetHeaders(received);
        HeaderValue(headers, "ce-type").Should().Be("tests.order.placed");
        headers.ContainsKey("cloudEvents:type").Should().BeFalse();
        HeaderValue(headers, "content-type").Should().Be("application/json");
        HeaderValue(headers, "content-encoding").Should().Be("gzip");
        HeaderValue(headers, "message-id").Should().Be("message-raw");
        HeaderValue(headers, "traceparent").Should().Be("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00");
    }

    [Fact]
    public async Task MessageIdDeduplicationStoresOnlyOneMessageForSameCloudEventId()
    {
        ServiceCollection services = new ();
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology => topology
                   .UseServer(_fixture.ConnectionString)
                   .Stream(
                        "ORDERS",
                        stream => stream.Subject("orders.*").DuplicateWindow(TimeSpan.FromMinutes(1))
                    )
                   .Publish<OrderPlaced>(
                        target => target.ToSubject("orders.dedup").UseMessageIdDeduplication()
                    )
            );
        await using var provider = services.BuildServiceProvider();

        foreach (var provisioner in provider.GetServices<ITopologyProvisioner>())
        {
            await provisioner.ProvisionAsync(TestContext.Current.CancellationToken);
        }

        await using NatsConnection connection = new (new NatsOpts { Url = _fixture.ConnectionString });
        NatsJSContext jetStream = new (connection);
        await jetStream.CreateOrUpdateConsumerAsync(
            "ORDERS",
            new ConsumerConfig("dedup-inspector")
            {
                DurableName = "dedup-inspector",
                FilterSubject = "orders.dedup",
                AckPolicy = ConsumerConfigAckPolicy.Explicit,
                DeliverPolicy = ConsumerConfigDeliverPolicy.All
            },
            TestContext.Current.CancellationToken
        );

        var publisher = provider.GetRequiredService<IMessagePublisher>();
        var eventId = Guid.Parse("89abfdcb-fb6f-4203-870f-9dd019fbc907");
        OrderPlaced message = new () { Id = eventId, OrderId = "order-dedup" };

        await publisher.PublishMessageAsync(message, cancellationToken: TestContext.Current.CancellationToken);
        await publisher.PublishMessageAsync(message, cancellationToken: TestContext.Current.CancellationToken);

        var received = await ReadMessagesAsync(
            jetStream,
            "ORDERS",
            "dedup-inspector",
            2,
            TimeSpan.FromSeconds(2),
            TestContext.Current.CancellationToken
        );

        received.Should().ContainSingle();
        HeaderValue(GetHeaders(received[0]), "message-id").Should().Be(eventId.ToString());
    }

    [Fact]
    public async Task UnknownDiscriminatorIsDeadLetteredAndOriginalDeliveryIsTerminated()
    {
        ServiceCollection services = new ();
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology => topology
                   .UseServer(_fixture.ConnectionString)
                   .Stream("ORDERS", stream => stream.Subject("orders.*"))
                   .Consume(
                        "ORDERS",
                        "orders-worker",
                        consumer => consumer
                           .FilterSubject("orders.unknown")
                           .DeadLetterSubject("orders.dead")
                           .Handle<OrderPlaced, OrderPlacedHandler>()
                    )
            );
        await using var provider = services.BuildServiceProvider();

        foreach (var provisioner in provider.GetServices<ITopologyProvisioner>())
        {
            await provisioner.ProvisionAsync(TestContext.Current.CancellationToken);
        }

        await using NatsConnection connection = new (new NatsOpts { Url = _fixture.ConnectionString });
        NatsJSContext jetStream = new (connection);
        await jetStream.CreateOrUpdateConsumerAsync(
            "ORDERS",
            new ConsumerConfig("dead-inspector")
            {
                DurableName = "dead-inspector",
                FilterSubject = "orders.dead",
                AckPolicy = ConsumerConfigAckPolicy.Explicit,
                DeliverPolicy = ConsumerConfigDeliverPolicy.All
            },
            TestContext.Current.CancellationToken
        );

        var runtimes = provider.GetServices<ITopologyRuntime>().ToArray();
        foreach (var runtime in runtimes)
        {
            await runtime.StartAsync(TestContext.Current.CancellationToken);
        }

        try
        {
            NatsHeaders headers = new ()
            {
                ["ce-type"] = "tests.unknown",
                ["message-id"] = "message-unknown"
            };
            await jetStream.PublishAsync(
                "orders.unknown",
                "unknown"u8.ToArray(),
                serializer: null,
                opts: null,
                headers,
                TestContext.Current.CancellationToken
            );

            var deadLetter = await ReadSingleMessageAsync(
                jetStream,
                "ORDERS",
                "dead-inspector",
                TestContext.Current.CancellationToken
            );

            deadLetter.Subject.Should().Be("orders.dead");
            deadLetter.Data.Should().Equal("unknown"u8.ToArray());
            HeaderValue(GetHeaders(deadLetter), "ce-type").Should().Be("tests.unknown");
        }
        finally
        {
            foreach (var runtime in runtimes)
            {
                await runtime.StopAsync(CancellationToken.None);
            }
        }
    }

    [Fact]
    public async Task UnknownDiscriminatorWithoutDeadLetterTerminatesOriginalDelivery()
    {
        ServiceCollection services = new ();
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology => topology
                   .UseServer(_fixture.ConnectionString)
                   .Stream("ORDERS", stream => stream.Subject("orders.*"))
                   .Consume(
                        "ORDERS",
                        "orders-terminate-worker",
                        consumer => consumer
                           .FilterSubject("orders.terminate")
                           .MaxDeliver(10)
                           .AckWait(TimeSpan.FromSeconds(1))
                           .Handle<OrderPlaced, OrderPlacedHandler>()
                    )
            );
        await using var provider = services.BuildServiceProvider();

        foreach (var provisioner in provider.GetServices<ITopologyProvisioner>())
        {
            await provisioner.ProvisionAsync(TestContext.Current.CancellationToken);
        }

        await using NatsConnection connection = new (new NatsOpts { Url = _fixture.ConnectionString });
        NatsJSContext jetStream = new (connection);
        var runtimes = provider.GetServices<ITopologyRuntime>().ToArray();
        foreach (var runtime in runtimes)
        {
            await runtime.StartAsync(TestContext.Current.CancellationToken);
        }

        try
        {
            NatsHeaders headers = new ()
            {
                ["ce-type"] = "tests.unknown",
                ["message-id"] = "message-terminate"
            };
            await jetStream.PublishAsync(
                "orders.terminate",
                "unknown"u8.ToArray(),
                serializer: null,
                opts: null,
                headers,
                TestContext.Current.CancellationToken
            );

            await Task.Delay(TimeSpan.FromMilliseconds(500), TestContext.Current.CancellationToken);
        }
        finally
        {
            foreach (var runtime in runtimes)
            {
                await runtime.StopAsync(CancellationToken.None);
            }
        }

        await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        var redelivered = await ReadMessagesAsync(
            jetStream,
            "ORDERS",
            "orders-terminate-worker",
            1,
            TimeSpan.FromSeconds(2),
            TestContext.Current.CancellationToken
        );
        redelivered.Should().BeEmpty();
    }

    [Fact]
    public async Task TypedPublishMapsCloudEventMetadataAndTraceContextForNatsWire()
    {
        ServiceCollection services = new ();
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology => topology
                   .UseServer(_fixture.ConnectionString)
                   .Stream("ORDERS", stream => stream.Subject("orders.*"))
                   .Publish<OrderPlaced>(target => target.ToSubject("orders.typed"))
            );
        await using var provider = services.BuildServiceProvider();

        foreach (var provisioner in provider.GetServices<ITopologyProvisioner>())
        {
            await provisioner.ProvisionAsync(TestContext.Current.CancellationToken);
        }

        await using NatsConnection connection = new (new NatsOpts { Url = _fixture.ConnectionString });
        NatsJSContext jetStream = new (connection);
        await jetStream.CreateOrUpdateConsumerAsync(
            "ORDERS",
            new ConsumerConfig("typed-inspector")
            {
                DurableName = "typed-inspector",
                FilterSubject = "orders.typed",
                AckPolicy = ConsumerConfigAckPolicy.Explicit,
                DeliverPolicy = ConsumerConfigDeliverPolicy.All
            },
            TestContext.Current.CancellationToken
        );

        // Listen to every activity source so the publish opens a recorded producer activity, which
        // TraceContextHeaders.Inject then propagates onto the NATS wire as a traceparent header.
        using ActivityListener listener = new ()
        {
            ShouldListenTo = static _ => true,
            Sample = static (ref _) =>
                ActivitySamplingResult.AllDataAndRecorded
        };
        ActivitySource.AddActivityListener(listener);

        var target = provider.GetRequiredService<NatsTopology>().GetRequiredTarget<OrderPlaced>();
        var eventId = Guid.Parse("6f5e4d3c-2b1a-4009-8f7e-6d5c4b3a2190");
        CloudEventMetadata metadata = new (eventId, DateTimeOffset.UtcNow, "orders/42");

        await target.PublishAsync(
            new OrderPlaced { OrderId = "order-typed" },
            in metadata,
            "tests.order.placed",
            "https://schemas.example/order",
            TestContext.Current.CancellationToken
        );

        var received = await ReadSingleMessageAsync(
            jetStream,
            "ORDERS",
            "typed-inspector",
            TestContext.Current.CancellationToken
        );
        received.Subject.Should().Be("orders.typed");
        var headers = GetHeaders(received);
        HeaderValue(headers, "ce-type").Should().Be("tests.order.placed");
        HeaderValue(headers, "ce-id").Should().Be(eventId.ToString());
        HeaderValue(headers, "ce-source").Should().Be("/tests");
        HeaderValue(headers, "ce-subject").Should().Be("orders/42");
        HeaderValue(headers, "ce-dataschema").Should().Be("https://schemas.example/order");
        HeaderValue(headers, "ce-specversion").Should().Be("1.0");
        HeaderValue(headers, "message-id").Should().Be(eventId.ToString());
        HeaderValue(headers, "content-type").Should().Contain("json");
        HeaderValue(headers, "traceparent").Should().StartWith("00-");
    }

    [Fact]
    public async Task TypedPublishMapsCloudEventExtensionsForNatsWire()
    {
        ServiceCollection services = new ();
        services.AddSingleton<ExtensionEmittingSerializer>();
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology => topology
                   .UseServer(_fixture.ConnectionString)
                   .Stream("ORDERS", stream => stream.Subject("orders.*"))
                   .Publish<OrderPlaced>(
                        target => target.ToSubject("orders.ext").WithSerializer<ExtensionEmittingSerializer>()
                    )
            );
        await using var provider = services.BuildServiceProvider();

        foreach (var provisioner in provider.GetServices<ITopologyProvisioner>())
        {
            await provisioner.ProvisionAsync(TestContext.Current.CancellationToken);
        }

        await using NatsConnection connection = new (new NatsOpts { Url = _fixture.ConnectionString });
        NatsJSContext jetStream = new (connection);
        await jetStream.CreateOrUpdateConsumerAsync(
            "ORDERS",
            new ConsumerConfig("ext-inspector")
            {
                DurableName = "ext-inspector",
                FilterSubject = "orders.ext",
                AckPolicy = ConsumerConfigAckPolicy.Explicit,
                DeliverPolicy = ConsumerConfigDeliverPolicy.All
            },
            TestContext.Current.CancellationToken
        );

        var target = provider.GetRequiredService<NatsTopology>().GetRequiredTarget<OrderPlaced>();

        await target.PublishAsync(
            new OrderPlaced { OrderId = "order-ext" },
            TestContext.Current.CancellationToken
        );

        var received = await ReadSingleMessageAsync(
            jetStream,
            "ORDERS",
            "ext-inspector",
            TestContext.Current.CancellationToken
        );
        received.Subject.Should().Be("orders.ext");
        var headers = GetHeaders(received);
        HeaderValue(headers, "ce-type").Should().Be("tests.order.placed");
        HeaderValue(headers, "ce-partitionkey").Should().Be("orders-42");
        HeaderValue(headers, "ce-tenant").Should().Be("acme");
    }

    private static async Task<INatsJSMsg<byte[]>> ReadSingleMessageAsync(
        NatsJSContext jetStream,
        string streamName,
        string durableName,
        CancellationToken cancellationToken
    )
    {
        var messages = await ReadMessagesAsync(
            jetStream,
            streamName,
            durableName,
            1,
            TimeSpan.FromSeconds(10),
            cancellationToken
        );

        return messages.Should().ContainSingle().Which;
    }

    private static async Task<List<INatsJSMsg<byte[]>>> ReadMessagesAsync(
        NatsJSContext jetStream,
        string streamName,
        string durableName,
        int maxMessages,
        TimeSpan timeout,
        CancellationToken cancellationToken
    )
    {
        using var timeoutSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutSource.CancelAfter(timeout);
        var consumer = await jetStream
           .GetConsumerAsync(streamName, durableName, timeoutSource.Token)
           .ConfigureAwait(false);
        NatsJSConsumeOpts options = new ()
        {
            MaxMsgs = maxMessages,
            Expires = timeout
        };
        List<INatsJSMsg<byte[]>> messages = [];
        try
        {
            await foreach (var message in consumer
                              .ConsumeAsync<byte[]>(serializer: null, options, timeoutSource.Token)
                              .ConfigureAwait(false))
            {
                messages.Add(message);
                await message.AckAsync(cancellationToken: timeoutSource.Token).ConfigureAwait(false);
                if (messages.Count == maxMessages)
                {
                    break;
                }
            }
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return messages;
        }

        return messages;
    }

    private static NatsHeaders GetHeaders(INatsJSMsg<byte[]> message)
    {
        message.Should().NotBeNull();
        var natsMessage = (INatsMsg) message;
        natsMessage.Headers.Should().NotBeNull();
        return natsMessage.Headers!;
    }

    private static string? HeaderValue(NatsHeaders headers, string name)
    {
        return headers.TryGetValue(name, out var value) ? value.ToString() : null;
    }

    private sealed class ExtensionEmittingSerializer : IMessageSerializer
    {
        public ValueTask<CloudEventEnvelope> SerializeAsync<T>(
            T message,
            in CloudEventMetadata metadata,
            string? type,
            string? dataSchema,
            CancellationToken cancellationToken = default
        )
        {
            Dictionary<string, string?> extensions = new (StringComparer.Ordinal)
            {
                ["partitionkey"] = "orders-42",
                ["tenant"] = "acme"
            };

            CloudEventEnvelope envelope = new (
                "1.0",
                metadata.Id.ToString(),
                metadata.Source ?? "/tests",
                type ?? "tests.order.placed",
                metadata.Time,
                metadata.Subject,
                "application/json",
                dataSchema,
                "{}"u8.ToArray(),
                extensions
            );

            return new ValueTask<CloudEventEnvelope>(envelope);
        }
    }

    private sealed class RecordingProbe
    {
        private readonly TaskCompletionSource<OrderPlaced> _received =
            new (TaskCreationOptions.RunContinuationsAsynchronously);

        public void Record(OrderPlaced message)
        {
            _received.TrySetResult(message);
        }

        public async Task<OrderPlaced> WaitAsync(TimeSpan timeout, CancellationToken cancellationToken)
        {
            var delay = Task.Delay(timeout, cancellationToken);
            var completed = await Task.WhenAny(_received.Task, delay).ConfigureAwait(false);
            if (completed == delay)
            {
                throw new TimeoutException("Timed out waiting for a NATS message.");
            }

            return await _received.Task.ConfigureAwait(false);
        }
    }

    private sealed class RedeliveryProbe
    {
        private readonly TaskCompletionSource<uint> _received =
            new (TaskCreationOptions.RunContinuationsAsynchronously);

        private int _seen;

        public void Observe(IncomingMessageContext context)
        {
            if (Interlocked.Increment(ref _seen) == 1)
            {
                throw new RetryMessageException();
            }

            _received.TrySetResult(context.Transport.DeliveryAttempt);
        }

        public async Task<uint> WaitAsync(TimeSpan timeout, CancellationToken cancellationToken)
        {
            var delay = Task.Delay(timeout, cancellationToken);
            var completed = await Task.WhenAny(_received.Task, delay).ConfigureAwait(false);
            if (completed == delay)
            {
                throw new TimeoutException("Timed out waiting for a NATS redelivery.");
            }

            return await _received.Task.ConfigureAwait(false);
        }
    }

    private sealed class SlowHandlerProbe
    {
        private readonly TaskCompletionSource<uint> _completed =
            new (TaskCreationOptions.RunContinuationsAsynchronously);

        private int _seen;

        public int InvocationCount => Volatile.Read(ref _seen);

        public async Task ObserveAsync(IncomingMessageContext context, CancellationToken cancellationToken)
        {
            var seen = Interlocked.Increment(ref _seen);
            if (seen == 1)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(3500), cancellationToken).ConfigureAwait(false);
                _completed.TrySetResult(context.Transport.DeliveryAttempt);
            }
        }

        public async Task<uint> WaitForCompletionAsync(TimeSpan timeout, CancellationToken cancellationToken)
        {
            var delay = Task.Delay(timeout, cancellationToken);
            var completed = await Task.WhenAny(_completed.Task, delay).ConfigureAwait(false);
            if (completed == delay)
            {
                throw new TimeoutException("Timed out waiting for the slow NATS handler.");
            }

            return await _completed.Task.ConfigureAwait(false);
        }
    }

    private sealed class ManualAckProbe
    {
        private readonly TaskCompletionSource<uint> _received =
            new (TaskCreationOptions.RunContinuationsAsynchronously);

        private int _seen;

        public int InvocationCount => Volatile.Read(ref _seen);

        public async Task AckAndRecordAsync(IncomingMessageContext context, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _seen);
            await context.Acknowledgement.AckAsync(cancellationToken).ConfigureAwait(false);
            _received.TrySetResult(context.Transport.DeliveryAttempt);
        }

        public async Task<uint> WaitAsync(TimeSpan timeout, CancellationToken cancellationToken)
        {
            var delay = Task.Delay(timeout, cancellationToken);
            var completed = await Task.WhenAny(_received.Task, delay).ConfigureAwait(false);
            if (completed == delay)
            {
                throw new TimeoutException("Timed out waiting for a manually acknowledged NATS message.");
            }

            return await _received.Task.ConfigureAwait(false);
        }
    }

    private sealed class ManualNoAckProbe
    {
        private readonly TaskCompletionSource<uint> _redelivered =
            new (TaskCreationOptions.RunContinuationsAsynchronously);

        private int _seen;

        public int InvocationCount => Volatile.Read(ref _seen);

        public void Observe(IncomingMessageContext context)
        {
            Interlocked.Increment(ref _seen);
            if (context.Transport.DeliveryAttempt > 1)
            {
                _redelivered.TrySetResult(context.Transport.DeliveryAttempt);
            }
        }

        public async Task<uint> WaitForRedeliveryAsync(TimeSpan timeout, CancellationToken cancellationToken)
        {
            var delay = Task.Delay(timeout, cancellationToken);
            var completed = await Task.WhenAny(_redelivered.Task, delay).ConfigureAwait(false);
            if (completed == delay)
            {
                throw new TimeoutException(
                    "Timed out waiting for an unsettled NATS manual acknowledgement redelivery."
                );
            }

            return await _redelivered.Task.ConfigureAwait(false);
        }
    }

    private sealed class NoProgressProbe
    {
        private readonly TaskCompletionSource<uint> _redelivered =
            new (TaskCreationOptions.RunContinuationsAsynchronously);

        private int _seen;

        public int InvocationCount => Volatile.Read(ref _seen);

        public async Task ObserveAsync(IncomingMessageContext context, CancellationToken cancellationToken)
        {
            var seen = Interlocked.Increment(ref _seen);
            if (seen == 1)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(2500), cancellationToken).ConfigureAwait(false);
                return;
            }

            _redelivered.TrySetResult(context.Transport.DeliveryAttempt);
        }

        public async Task<uint> WaitForRedeliveryAsync(TimeSpan timeout, CancellationToken cancellationToken)
        {
            var delay = Task.Delay(timeout, cancellationToken);
            var completed = await Task.WhenAny(_redelivered.Task, delay).ConfigureAwait(false);
            if (completed == delay)
            {
                throw new TimeoutException("Timed out waiting for NATS redelivery without AckProgress.");
            }

            return await _redelivered.Task.ConfigureAwait(false);
        }
    }

    private sealed class MultiMessageProbe
    {
        private readonly TaskCompletionSource<OrderCancelled> _cancelled =
            new (TaskCreationOptions.RunContinuationsAsynchronously);

        private readonly TaskCompletionSource<OrderPlaced> _placed =
            new (TaskCreationOptions.RunContinuationsAsynchronously);

        public void Record(OrderPlaced message)
        {
            _placed.TrySetResult(message);
        }

        public void Record(OrderCancelled message)
        {
            _cancelled.TrySetResult(message);
        }

        public async Task<OrderPlaced> WaitForPlacedAsync(TimeSpan timeout, CancellationToken cancellationToken)
        {
            var delay = Task.Delay(timeout, cancellationToken);
            var completed = await Task.WhenAny(_placed.Task, delay).ConfigureAwait(false);
            if (completed == delay)
            {
                throw new TimeoutException("Timed out waiting for the placed NATS message.");
            }

            return await _placed.Task.ConfigureAwait(false);
        }

        public async Task<OrderCancelled> WaitForCancelledAsync(TimeSpan timeout, CancellationToken cancellationToken)
        {
            var delay = Task.Delay(timeout, cancellationToken);
            var completed = await Task.WhenAny(_cancelled.Task, delay).ConfigureAwait(false);
            if (completed == delay)
            {
                throw new TimeoutException("Timed out waiting for the cancelled NATS message.");
            }

            return await _cancelled.Task.ConfigureAwait(false);
        }
    }

    private sealed class RecordingOrderPlacedHandler : IMessageHandler<OrderPlaced>
    {
        private readonly RecordingProbe _probe;

        public RecordingOrderPlacedHandler(RecordingProbe probe)
        {
            _probe = probe;
        }

        public Task HandleAsync(
            OrderPlaced message,
            IncomingMessageContext context,
            CancellationToken cancellationToken = default
        )
        {
            _probe.Record(message);
            return Task.CompletedTask;
        }
    }

    private sealed class MultiOrderPlacedHandler : IMessageHandler<OrderPlaced>
    {
        private readonly MultiMessageProbe _probe;

        public MultiOrderPlacedHandler(MultiMessageProbe probe)
        {
            _probe = probe;
        }

        public Task HandleAsync(
            OrderPlaced message,
            IncomingMessageContext context,
            CancellationToken cancellationToken = default
        )
        {
            _probe.Record(message);
            return Task.CompletedTask;
        }
    }

    private sealed class MultiOrderCancelledHandler : IMessageHandler<OrderCancelled>
    {
        private readonly MultiMessageProbe _probe;

        public MultiOrderCancelledHandler(MultiMessageProbe probe)
        {
            _probe = probe;
        }

        public Task HandleAsync(
            OrderCancelled message,
            IncomingMessageContext context,
            CancellationToken cancellationToken = default
        )
        {
            _probe.Record(message);
            return Task.CompletedTask;
        }
    }

    private sealed class SlowOrderPlacedHandler : IMessageHandler<OrderPlaced>
    {
        private readonly SlowHandlerProbe _probe;

        public SlowOrderPlacedHandler(SlowHandlerProbe probe)
        {
            _probe = probe;
        }

        public Task HandleAsync(
            OrderPlaced message,
            IncomingMessageContext context,
            CancellationToken cancellationToken = default
        )
        {
            return _probe.ObserveAsync(context, cancellationToken);
        }
    }

    private sealed class NoProgressOrderPlacedHandler : IMessageHandler<OrderPlaced>
    {
        private readonly NoProgressProbe _probe;

        public NoProgressOrderPlacedHandler(NoProgressProbe probe)
        {
            _probe = probe;
        }

        public Task HandleAsync(
            OrderPlaced message,
            IncomingMessageContext context,
            CancellationToken cancellationToken = default
        )
        {
            return _probe.ObserveAsync(context, cancellationToken);
        }
    }

    private sealed class ManualAckOrderPlacedHandler : IMessageHandler<OrderPlaced>
    {
        private readonly ManualAckProbe _probe;

        public ManualAckOrderPlacedHandler(ManualAckProbe probe)
        {
            _probe = probe;
        }

        public Task HandleAsync(
            OrderPlaced message,
            IncomingMessageContext context,
            CancellationToken cancellationToken = default
        )
        {
            return _probe.AckAndRecordAsync(context, cancellationToken);
        }
    }

    private sealed class ManualNoAckOrderPlacedHandler : IMessageHandler<OrderPlaced>
    {
        private readonly ManualNoAckProbe _probe;

        public ManualNoAckOrderPlacedHandler(ManualNoAckProbe probe)
        {
            _probe = probe;
        }

        public Task HandleAsync(
            OrderPlaced message,
            IncomingMessageContext context,
            CancellationToken cancellationToken = default
        )
        {
            _probe.Observe(context);
            return Task.CompletedTask;
        }
    }

    private sealed class FailingOrderPlacedHandler : IMessageHandler<OrderPlaced>
    {
        public Task HandleAsync(
            OrderPlaced message,
            IncomingMessageContext context,
            CancellationToken cancellationToken = default
        )
        {
            throw new InvalidOperationException("Simulated handler failure.");
        }
    }

    private sealed class RedeliveringOrderPlacedHandler : IMessageHandler<OrderPlaced>
    {
        private readonly RedeliveryProbe _probe;

        public RedeliveringOrderPlacedHandler(RedeliveryProbe probe)
        {
            _probe = probe;
        }

        public Task HandleAsync(
            OrderPlaced message,
            IncomingMessageContext context,
            CancellationToken cancellationToken = default
        )
        {
            _probe.Observe(context);
            return Task.CompletedTask;
        }
    }
}
