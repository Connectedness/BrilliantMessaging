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
    public async Task HandlerFailureDeadLettersWhenDeduplicationSharesTheStream()
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
                        target => target.ToSubject("orders.dedup-dead").UseMessageIdDeduplication()
                    )
                   .Consume(
                        "ORDERS",
                        "orders-dedup-dead-worker",
                        consumer => consumer
                           .FilterSubject("orders.dedup-dead")
                           .MaxDeliver(1)
                           .DeadLetterSubject("orders.dead")
                           .Handle<OrderPlaced, FailingOrderPlacedHandler>()
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
            new ConsumerConfig("dedup-dead-inspector")
            {
                DurableName = "dedup-dead-inspector",
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
            var publisher = provider.GetRequiredService<IMessagePublisher>();
            var eventId = Guid.Parse("0d0186a1-64f5-4d29-9bf5-2f4c19fbe905");
            await publisher.PublishMessageAsync(
                new OrderPlaced { Id = eventId, OrderId = "order-dedup-dead" },
                cancellationToken: TestContext.Current.CancellationToken
            );

            // The dead-letter subject lives in the same stream as the original, whose Nats-Msg-Id sits
            // inside the duplicate window; only a copy published under a derived id can be stored.
            var deadLetter = await ReadSingleMessageAsync(
                jetStream,
                "ORDERS",
                "dedup-dead-inspector",
                TestContext.Current.CancellationToken
            );

            deadLetter.Subject.Should().Be("orders.dead");
            var headers = GetHeaders(deadLetter);
            HeaderValue(headers, "ce-id").Should().Be(eventId.ToString());
            HeaderValue(headers, "Nats-Msg-Id")
               .Should()
               .Be($"{eventId}:dlq:orders-dedup-dead-worker:orders.dead");
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
    public async Task RetriedDeadLetterPublishTreatsStoredDuplicateAsSuccess()
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
                        target => target.ToSubject("orders.retry-dead").UseMessageIdDeduplication()
                    )
                   .Consume(
                        "ORDERS",
                        "orders-retry-dead-worker",
                        consumer => consumer
                           .FilterSubject("orders.retry-dead")
                           .MaxDeliver(1)
                           .AckWait(TimeSpan.FromSeconds(30))
                           .DeadLetterSubject("orders.dead")
                           .Handle<OrderPlaced, FailingOrderPlacedHandler>()
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
            new ConsumerConfig("retry-dead-inspector")
            {
                DurableName = "retry-dead-inspector",
                FilterSubject = "orders.dead",
                AckPolicy = ConsumerConfigAckPolicy.Explicit,
                DeliverPolicy = ConsumerConfigDeliverPolicy.All
            },
            TestContext.Current.CancellationToken
        );

        // Simulates an earlier dead-letter publish whose subsequent terminate failed: the copy is already
        // stored under the derived id (format pinned by NatsMessageMappingTests) when the redelivered
        // original goes through the dead-letter sequence again.
        var eventId = Guid.Parse("5b3ff6dc-8a04-4baf-95cd-71a0e5cb02b6");
        var derivedMessageId = $"{eventId}:dlq:orders-retry-dead-worker:orders.dead";
        var storedCopy = await jetStream.PublishAsync(
            "orders.dead",
            "stored-copy"u8.ToArray(),
            serializer: null,
            opts: new NatsJSPubOpts { MsgId = derivedMessageId },
            headers: null,
            TestContext.Current.CancellationToken
        );
        storedCopy.EnsureSuccess();

        var runtimes = provider.GetServices<ITopologyRuntime>().ToArray();
        foreach (var runtime in runtimes)
        {
            await runtime.StartAsync(TestContext.Current.CancellationToken);
        }

        try
        {
            var publisher = provider.GetRequiredService<IMessagePublisher>();
            await publisher.PublishMessageAsync(
                new OrderPlaced { Id = eventId, OrderId = "order-retry-dead" },
                cancellationToken: TestContext.Current.CancellationToken
            );

            // The duplicate acknowledgement for the derived id must count as success so the original is
            // terminated right away. AckWait is 30s, so pending clearing well within that window can only
            // come from the terminate - a failed dead-letter publish would leave the delivery pending for
            // the full AckWait.
            await WaitForNoAckPendingAsync(
                jetStream,
                "ORDERS",
                "orders-retry-dead-worker",
                TimeSpan.FromSeconds(10),
                TestContext.Current.CancellationToken
            );

            var deadLetters = await ReadMessagesAsync(
                jetStream,
                "ORDERS",
                "retry-dead-inspector",
                2,
                TimeSpan.FromSeconds(2),
                TestContext.Current.CancellationToken
            );
            deadLetters.Should().ContainSingle().Which.Data.Should().Equal("stored-copy"u8.ToArray());
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
    public async Task TopologyLocalMessageContractsAreResolvedOnInboundDispatch()
    {
        RecordingProbe probe = new ();
        ServiceCollection services = new ();
        services.AddSingleton(probe);
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .AddNatsTopology(
                topology => topology
                   .UseServer(_fixture.ConnectionString)
                   .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.local.order.placed"))
                   .Stream("ORDERS", stream => stream.Subject("orders.*"))
                   .Publish<OrderPlaced>(target => target.ToSubject("orders.local"))
                   .Consume(
                        "ORDERS",
                        "orders-local-worker",
                        consumer => consumer
                           .FilterSubject("orders.local")
                           .MaxDeliver(1)
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
                new OrderPlaced { OrderId = "order-local" },
                cancellationToken: TestContext.Current.CancellationToken
            );

            // The contract exists only in the topology dialect; inbound dispatch must resolve it through the
            // topology's effective registry rather than the globally registered inspector.
            var received = await probe.WaitAsync(TimeSpan.FromSeconds(15), TestContext.Current.CancellationToken);
            received.OrderId.Should().Be("order-local");
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
    public async Task GlobalMessageContractsResolveWhenTopologyDeclaresLocalContracts()
    {
        MultiMessageProbe probe = new ();
        ServiceCollection services = new ();
        services.AddSingleton(probe);
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology => topology
                   .UseServer(_fixture.ConnectionString)
                   .MapMessageContracts(contracts => contracts.Map<OrderCancelled>("tests.local.order.cancelled"))
                   .Stream("ORDERS", stream => stream.Subject("orders.*"))
                   .Publish<OrderPlaced>(target => target.ToSubject("orders.mixed"))
                   .Publish<OrderCancelled>(target => target.ToSubject("orders.mixed"))
                   .Consume(
                        "ORDERS",
                        "orders-mixed-worker",
                        consumer => consumer
                           .FilterSubject("orders.mixed")
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
                new OrderPlaced { OrderId = "order-mixed-placed" },
                cancellationToken: TestContext.Current.CancellationToken
            );
            await publisher.PublishMessageAsync(
                new OrderCancelled { OrderId = "order-mixed-cancelled" },
                cancellationToken: TestContext.Current.CancellationToken
            );

            // The topology dialect must extend the global contracts, not replace them: the globally mapped
            // type and the topology-local type are both dispatched by the same consumer.
            var placed = await probe.WaitForPlacedAsync(
                TimeSpan.FromSeconds(15),
                TestContext.Current.CancellationToken
            );
            var cancelled = await probe.WaitForCancelledAsync(
                TimeSpan.FromSeconds(15),
                TestContext.Current.CancellationToken
            );
            placed.OrderId.Should().Be("order-mixed-placed");
            cancelled.OrderId.Should().Be("order-mixed-cancelled");
        }
        finally
        {
            foreach (var runtime in runtimes)
            {
                await runtime.StopAsync(CancellationToken.None);
            }
        }
    }

    [Theory]
    [InlineData("ce-id", null)]
    [InlineData("ce-time", "not-a-timestamp")]
    public async Task MalformedCloudEventWithKnownTypeIsDeadLetteredAndOriginalDeliveryIsTerminated(
        string headerName,
        string? headerValue
    )
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
                        "orders-malformed-worker",
                        consumer => consumer
                           .FilterSubject("orders.malformed")
                           .MaxDeliver(3)
                           .AckWait(TimeSpan.FromSeconds(30))
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
            new ConsumerConfig("malformed-dead-inspector")
            {
                DurableName = "malformed-dead-inspector",
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
            // A complete, valid envelope for the known contract, then broken in exactly one attribute.
            NatsHeaders headers = new ()
            {
                ["ce-specversion"] = "1.0",
                ["ce-id"] = "malformed-1",
                ["ce-source"] = "/external-producer",
                ["ce-type"] = "tests.order.placed",
                ["ce-time"] = "2026-07-19T08:00:00Z",
                ["content-type"] = "application/json"
            };
            if (headerValue is null)
            {
                headers.Remove(headerName);
            }
            else
            {
                headers[headerName] = headerValue;
            }

            await jetStream.PublishAsync(
                "orders.malformed",
                "malformed"u8.ToArray(),
                serializer: null,
                opts: null,
                headers,
                TestContext.Current.CancellationToken
            );

            // AckWait is 30s; the delivery settling well within that window proves the inspection failure
            // was routed through dead-letter/terminate instead of stranding the message unsettled.
            await WaitForNoAckPendingAsync(
                jetStream,
                "ORDERS",
                "orders-malformed-worker",
                TimeSpan.FromSeconds(10),
                TestContext.Current.CancellationToken
            );

            var deadLetter = await ReadSingleMessageAsync(
                jetStream,
                "ORDERS",
                "malformed-dead-inspector",
                TestContext.Current.CancellationToken
            );

            deadLetter.Subject.Should().Be("orders.dead");
            deadLetter.Data.Should().Equal("malformed"u8.ToArray());
            HeaderValue(GetHeaders(deadLetter), "ce-type").Should().Be("tests.order.placed");
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
    public async Task ShutdownInterruptedDeliveryOnFinalAttemptIsRedeliveredAndProcessed()
    {
        InterruptionProbe probe = new ();
        ServiceCollection services = new ();
        services.AddSingleton(probe);
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology => topology
                   .UseServer(_fixture.ConnectionString)
                   .Stream("ORDERS", stream => stream.Subject("orders.*"))
                   .Publish<OrderPlaced>(target => target.ToSubject("orders.interrupted"))
                   .Consume(
                        "ORDERS",
                        "orders-interrupted-worker",
                        consumer => consumer
                           .FilterSubject("orders.interrupted")
                           .MaxDeliver(1)
                           .AckWait(TimeSpan.FromSeconds(30))
                           .DeadLetterSubject("orders.dead")
                           .Handle<OrderPlaced, InterruptibleOrderPlacedHandler>()
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
            new ConsumerConfig("interrupted-dead-inspector")
            {
                DurableName = "interrupted-dead-inspector",
                FilterSubject = "orders.dead",
                AckPolicy = ConsumerConfigAckPolicy.Explicit,
                DeliverPolicy = ConsumerConfigDeliverPolicy.All
            },
            TestContext.Current.CancellationToken
        );

        var runtimes = provider.GetServices<ITopologyRuntime>().ToArray();
        try
        {
            foreach (var runtime in runtimes)
            {
                await runtime.StartAsync(TestContext.Current.CancellationToken);
            }

            var publisher = provider.GetRequiredService<IMessagePublisher>();
            await publisher.PublishMessageAsync(
                new OrderPlaced { OrderId = "order-interrupted" },
                cancellationToken: TestContext.Current.CancellationToken
            );

            await probe.WaitForStartAsync(TimeSpan.FromSeconds(15), TestContext.Current.CancellationToken);

            // MaxDeliver is 1, so this stop interrupts the delivery on its final configured attempt.
            // The interruption must not consume the retry policy: after a restart the message has to be
            // processed normally instead of being dead-lettered.
            foreach (var runtime in runtimes)
            {
                await runtime.StopAsync(CancellationToken.None);
            }

            foreach (var runtime in runtimes)
            {
                await runtime.StartAsync(TestContext.Current.CancellationToken);
            }

            var processed = await probe.WaitForSuccessAsync(
                TimeSpan.FromSeconds(15),
                TestContext.Current.CancellationToken
            );
            processed.OrderId.Should().Be("order-interrupted");

            var deadLetters = await ReadMessagesAsync(
                jetStream,
                "ORDERS",
                "interrupted-dead-inspector",
                1,
                TimeSpan.FromSeconds(2),
                TestContext.Current.CancellationToken
            );
            deadLetters.Should().BeEmpty();
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
    public async Task ShutdownInterruptionsBeyondServerHeadroomDeadLetterTheDelivery()
    {
        BlockingProbe probe = new ();
        ServiceCollection services = new ();
        services.AddSingleton(probe);
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology => topology
                   .UseServer(_fixture.ConnectionString)
                   .Stream("ORDERS", stream => stream.Subject("orders.*"))
                   .Publish<OrderPlaced>(target => target.ToSubject("orders.storm"))
                   .Consume(
                        "ORDERS",
                        "orders-storm-worker",
                        consumer => consumer
                           .FilterSubject("orders.storm")
                           .MaxDeliver(1)
                           .AckWait(TimeSpan.FromSeconds(30))
                           .DeadLetterSubject("orders.dead")
                           .Handle<OrderPlaced, BlockingOrderPlacedHandler>()
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
            new ConsumerConfig("storm-dead-inspector")
            {
                DurableName = "storm-dead-inspector",
                FilterSubject = "orders.dead",
                AckPolicy = ConsumerConfigAckPolicy.Explicit,
                DeliverPolicy = ConsumerConfigDeliverPolicy.All
            },
            TestContext.Current.CancellationToken
        );

        var runtimes = provider.GetServices<ITopologyRuntime>().ToArray();
        try
        {
            var publisher = provider.GetRequiredService<IMessagePublisher>();

            // MaxDeliver(1) provisions a server-side limit of 2, so the second interruption exhausts the
            // redelivery headroom and must dead-letter the message instead of stranding it with a NAK the
            // server would ignore.
            for (var cycle = 0; cycle < 2; cycle++)
            {
                foreach (var runtime in runtimes)
                {
                    await runtime.StartAsync(TestContext.Current.CancellationToken);
                }

                if (cycle == 0)
                {
                    await publisher.PublishMessageAsync(
                        new OrderPlaced { OrderId = "order-storm" },
                        cancellationToken: TestContext.Current.CancellationToken
                    );
                }

                await probe.WaitForStartAsync(TimeSpan.FromSeconds(15), TestContext.Current.CancellationToken);
                foreach (var runtime in runtimes)
                {
                    await runtime.StopAsync(CancellationToken.None);
                }
            }

            var deadLetter = await ReadSingleMessageAsync(
                jetStream,
                "ORDERS",
                "storm-dead-inspector",
                TestContext.Current.CancellationToken
            );
            deadLetter.Subject.Should().Be("orders.dead");
            probe.Invocations.Should().Be(2);
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
    public async Task CanonicallyCasedHeadersFromExternalProducerAreResolved()
    {
        EnvelopeProbe probe = new ();
        ServiceCollection services = new ();
        services.AddSingleton(probe);
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology => topology
                   .UseServer(_fixture.ConnectionString)
                   .Stream("ORDERS", stream => stream.Subject("orders.*"))
                   .Consume(
                        "ORDERS",
                        "orders-canonical-worker",
                        consumer => consumer
                           .FilterSubject("orders.canonical")
                           .MaxDeliver(1)
                           .Handle<OrderPlaced, EnvelopeRecordingOrderPlacedHandler>()
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
            // HTTP-style canonical casing, as an external (non-Brilliant-Messaging) producer would send it.
            NatsHeaders headers = new ()
            {
                ["Ce-Specversion"] = "1.0",
                ["Ce-Id"] = "canonical-1",
                ["Ce-Source"] = "/external-producer",
                ["Ce-Type"] = "tests.order.placed",
                ["Ce-Time"] = "2026-07-19T08:00:00Z",
                ["Content-Type"] = "application/json"
            };
            await jetStream.PublishAsync(
                "orders.canonical",
                """{"OrderId":"order-canonical"}"""u8.ToArray(),
                serializer: null,
                opts: null,
                headers,
                TestContext.Current.CancellationToken
            );

            var (message, envelope, contentType) = await probe.WaitAsync(
                TimeSpan.FromSeconds(15),
                TestContext.Current.CancellationToken
            );

            message.OrderId.Should().Be("order-canonical");
            contentType.Should().Be("application/json");
            envelope.HasValue.Should().BeTrue();
            envelope!.Value.Id.Should().Be("canonical-1");
            // The mis-cased core attributes must be canonicalized during mapping, not surface as bogus
            // extension attributes named "Type", "Id", etc.
            envelope.Value.Extensions.Should().BeNull();
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
    public async Task ManualAckHandlerExceptionDeadLettersViaOuterSafetyNet()
    {
        ServiceCollection services = new ();
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology => topology
                   .UseServer(_fixture.ConnectionString)
                   .Stream("ORDERS", stream => stream.Subject("orders.*"))
                   .Publish<OrderPlaced>(target => target.ToSubject("orders.manual-throw"))
                   .Consume(
                        "ORDERS",
                        "orders-manual-throw-worker",
                        consumer => consumer
                           .FilterSubject("orders.manual-throw")
                           .MaxDeliver(5)
                           .AckWait(TimeSpan.FromSeconds(30))
                           .DeadLetterSubject("orders.dead")
                           .Handle<OrderPlaced, ThrowingManualAckOrderPlacedHandler>(handler => handler.ManualAck())
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
            new ConsumerConfig("manual-throw-dead-inspector")
            {
                DurableName = "manual-throw-dead-inspector",
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
            var publisher = provider.GetRequiredService<IMessagePublisher>();
            await publisher.PublishMessageAsync(
                new OrderPlaced { OrderId = "order-manual-throw" },
                cancellationToken: TestContext.Current.CancellationToken
            );

            // AckWait is 30s, so a dead-letter arriving well within that window can only come from the outer
            // safety-net nack (requeue: false) firing on the manual-mode handler exception - the framework
            // acknowledgement middleware leaves manual-mode deliveries unsettled - rather than from an
            // AckWait-timeout redelivery.
            var deadLetter = await ReadSingleMessageAsync(
                jetStream,
                "ORDERS",
                "manual-throw-dead-inspector",
                TestContext.Current.CancellationToken
            );

            deadLetter.Subject.Should().Be("orders.dead");
            HeaderValue(GetHeaders(deadLetter), "ce-type").Should().Be("tests.order.placed");
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
    public async Task UnrecognizedDeliveryRecordsConsumedMessageMetricWithOtherErrorType()
    {
        using InboundDiagnosticsRecorder recorder = new ();
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
                        "orders-metric-unknown-worker",
                        consumer => consumer
                           .FilterSubject("orders.metric-unknown")
                           .MaxDeliver(1)
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
                ["message-id"] = "message-metric-unknown"
            };
            await jetStream.PublishAsync(
                "orders.metric-unknown",
                "unknown"u8.ToArray(),
                serializer: null,
                opts: null,
                headers,
                TestContext.Current.CancellationToken
            );

            var measurement = await WaitForConsumedMeasurementAsync(
                recorder,
                "orders.metric-unknown",
                TimeSpan.FromSeconds(15),
                TestContext.Current.CancellationToken
            );

            TagValue(measurement, MessagingSemanticConventions.MessagingSystem).Should().Be("nats");
            TagValue(measurement, MessagingSemanticConventions.MessagingOperationName)
               .Should()
               .Be(MessagingSemanticConventions.ProcessOperation);
            TagValue(measurement, MessagingSemanticConventions.ErrorType)
               .Should()
               .Be(MessagingSemanticConventions.ErrorTypeOther);
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
        using ActivityListener listener = new ();
        listener.ShouldListenTo = static _ => true;
        listener.Sample = static (ref _) =>
            ActivitySamplingResult.AllDataAndRecorded;
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

    private static async Task WaitForNoAckPendingAsync(
        NatsJSContext jetStream,
        string streamName,
        string durableName,
        TimeSpan timeout,
        CancellationToken cancellationToken
    )
    {
        var deadline = DateTimeOffset.UtcNow + timeout;
        while (DateTimeOffset.UtcNow < deadline)
        {
            var consumer = await jetStream
               .GetConsumerAsync(streamName, durableName, cancellationToken)
               .ConfigureAwait(false);
            if (consumer.Info.NumAckPending == 0)
            {
                return;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(200), cancellationToken).ConfigureAwait(false);
        }

        throw new TimeoutException(
            $"Timed out waiting for consumer '{durableName}' to have no acknowledgements pending."
        );
    }

    private static NatsHeaders GetHeaders(INatsJSMsg<byte[]> message)
    {
        message.Should().NotBeNull();
        INatsMsg natsMessage = message;
        natsMessage.Headers.Should().NotBeNull();
        return natsMessage.Headers!;
    }

    private static string? HeaderValue(NatsHeaders headers, string name)
    {
        return headers.TryGetValue(name, out var value) ? value.ToString() : null;
    }

    private static async Task<KeyValuePair<string, object?>[]> WaitForConsumedMeasurementAsync(
        InboundDiagnosticsRecorder recorder,
        string destination,
        TimeSpan timeout,
        CancellationToken cancellationToken
    )
    {
        var deadline = DateTimeOffset.UtcNow + timeout;
        while (DateTimeOffset.UtcNow < deadline)
        {
            var measurement = recorder
               .SnapshotConsumedMessages()
               .FirstOrDefault(
                    tags => string.Equals(
                        TagValue(tags, MessagingSemanticConventions.MessagingDestinationName),
                        destination,
                        StringComparison.Ordinal
                    )
                );
            if (measurement is not null)
            {
                return measurement;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(100), cancellationToken).ConfigureAwait(false);
        }

        throw new TimeoutException(
            $"Timed out waiting for a consumed-messages measurement for destination '{destination}'."
        );
    }

    private static string? TagValue(KeyValuePair<string, object?>[] tags, string key)
    {
        foreach (var tag in tags)
        {
            if (string.Equals(tag.Key, key, StringComparison.Ordinal))
            {
                return tag.Value?.ToString();
            }
        }

        return null;
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

    private sealed class ThrowingManualAckOrderPlacedHandler : IMessageHandler<OrderPlaced>
    {
        public Task HandleAsync(
            OrderPlaced message,
            IncomingMessageContext context,
            CancellationToken cancellationToken = default
        )
        {
            throw new InvalidOperationException("Simulated manual-ack handler failure.");
        }
    }

    private sealed class EnvelopeProbe
    {
        private readonly TaskCompletionSource<(OrderPlaced Message, CloudEventEnvelope? Envelope, string? ContentType)>
            _received = new (TaskCreationOptions.RunContinuationsAsynchronously);

        public void Record(OrderPlaced message, IncomingMessageContext context)
        {
            context.Items.TryGetItem(CloudEventsContextKeys.Envelope, out var envelope);
            _received.TrySetResult((message, envelope, context.Transport.ContentType));
        }

        public async Task<(OrderPlaced Message, CloudEventEnvelope? Envelope, string? ContentType)> WaitAsync(
            TimeSpan timeout,
            CancellationToken cancellationToken
        )
        {
            return await _received.Task.WaitAsync(timeout, cancellationToken).ConfigureAwait(false);
        }
    }

    private sealed class EnvelopeRecordingOrderPlacedHandler : IMessageHandler<OrderPlaced>
    {
        private readonly EnvelopeProbe _probe;

        public EnvelopeRecordingOrderPlacedHandler(EnvelopeProbe probe)
        {
            _probe = probe;
        }

        public Task HandleAsync(
            OrderPlaced message,
            IncomingMessageContext context,
            CancellationToken cancellationToken = default
        )
        {
            _probe.Record(message, context);
            return Task.CompletedTask;
        }
    }

    private sealed class InterruptionProbe
    {
        private readonly TaskCompletionSource _started = new (TaskCreationOptions.RunContinuationsAsynchronously);

        private readonly TaskCompletionSource<OrderPlaced> _succeeded =
            new (TaskCreationOptions.RunContinuationsAsynchronously);

        private int _invocations;

        public async Task HandleAsync(OrderPlaced message, CancellationToken cancellationToken)
        {
            if (Interlocked.Increment(ref _invocations) == 1)
            {
                _started.TrySetResult();
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
                return;
            }

            _succeeded.TrySetResult(message);
        }

        public Task WaitForStartAsync(TimeSpan timeout, CancellationToken cancellationToken)
        {
            return _started.Task.WaitAsync(timeout, cancellationToken);
        }

        public Task<OrderPlaced> WaitForSuccessAsync(TimeSpan timeout, CancellationToken cancellationToken)
        {
            return _succeeded.Task.WaitAsync(timeout, cancellationToken);
        }
    }

    private sealed class InterruptibleOrderPlacedHandler : IMessageHandler<OrderPlaced>
    {
        private readonly InterruptionProbe _probe;

        public InterruptibleOrderPlacedHandler(InterruptionProbe probe)
        {
            _probe = probe;
        }

        public Task HandleAsync(
            OrderPlaced message,
            IncomingMessageContext context,
            CancellationToken cancellationToken = default
        )
        {
            return _probe.HandleAsync(message, cancellationToken);
        }
    }

    private sealed class BlockingProbe
    {
        private readonly SemaphoreSlim _started = new (0);

        public int Invocations;

        public async Task HandleAsync(CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref Invocations);
            _started.Release();
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
        }

        public Task WaitForStartAsync(TimeSpan timeout, CancellationToken cancellationToken)
        {
            return _started.WaitAsync(timeout, cancellationToken);
        }
    }

    private sealed class BlockingOrderPlacedHandler : IMessageHandler<OrderPlaced>
    {
        private readonly BlockingProbe _probe;

        public BlockingOrderPlacedHandler(BlockingProbe probe)
        {
            _probe = probe;
        }

        public Task HandleAsync(
            OrderPlaced message,
            IncomingMessageContext context,
            CancellationToken cancellationToken = default
        )
        {
            return _probe.HandleAsync(cancellationToken);
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
