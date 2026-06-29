using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;
using BrilliantMessaging.Core.Messaging.Outbound;
using BrilliantMessaging.Transport.InMemory.Outbound;
using BrilliantMessaging.Transport.InMemory.Tests.TestSupport;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace BrilliantMessaging.Transport.InMemory.Tests;

public sealed class InMemoryTransportTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);
    private static CancellationToken CancellationToken => TestContext.Current.CancellationToken;

    [Fact]
    public async Task PublishMessageAsync_RecordsSerializedMessageAndDispatchesThroughPipeline()
    {
        await using var host = await InMemoryTestHost
           .StartAsync(
                builder => WithContracts(builder)
                   .AddInMemoryTopology(
                        topology => topology
                           .Topic("orders")
                           .Publish<OrderPlaced>(target => target.ToTopic("orders"))
                           .Consume("orders", consumer => consumer.Handle<OrderPlaced, OrderPlacedHandler>())
                    )
            );

        await host.Publisher
           .PublishMessageAsync(new OrderPlaced { OrderId = "order-1" }, cancellationToken: CancellationToken);
        await host.Broker.DrainUntilIdleAsync(Timeout, CancellationToken);

        var invocation = host.Probe.Invocations.Should().ContainSingle().Which;
        invocation.Route.Should().Be("orders");
        invocation.Message.Should().BeOfType<OrderPlaced>().Which.OrderId.Should().Be("order-1");

        var recorded = host.Broker.GetMessages("orders").Should().ContainSingle().Which;
        recorded.Topic.Should().Be("orders");
        recorded.Body.Length.Should().BeGreaterThan(0);
        recorded.Headers[$"{InMemoryOutboundTarget<OrderPlaced>.CloudEventsHeaderPrefix}type"]
           .Should()
           .Be("tests.order.placed");
    }

    [Fact]
    public async Task PublishRawAsync_RecordsSerializedBodyHeadersContentTypeAndMessageId()
    {
        await using var serviceProvider = CreateOutboundOnlyServiceProvider();
        var publisher = serviceProvider.GetRequiredService<IMessagePublisher>();
        var target = serviceProvider.GetRequiredService<Topology>().GetRequiredTarget<OrderPlaced>();
        SerializedMessage message = new (
            "raw-body"u8.ToArray(),
            "application/octet-stream",
            "gzip",
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                ["x-custom"] = "custom-value",
                ["x-null"] = null
            },
            "raw-message-id",
            "raw-correlation-id"
        );

        await publisher.PublishRawAsync(message, target, CancellationToken);

        var recorded = serviceProvider.GetRequiredService<InMemoryBroker>()
           .GetMessages("orders")
           .Should()
           .ContainSingle()
           .Which;
        recorded.Topic.Should().Be("orders");
        recorded.Body.ToArray().Should().Equal("raw-body"u8.ToArray());
        recorded.ContentType.Should().Be("application/octet-stream");
        recorded.MessageId.Should().Be("raw-message-id");
        recorded.Headers.Should().ContainKey("x-custom").WhoseValue.Should().Be("custom-value");
        recorded.Headers.Should().ContainKey("x-null").WhoseValue.Should().BeNull();
    }

    [Fact]
    public async Task PublishRawAsync_ThrowsAndRecordsNothingWhenCancellationIsAlreadyRequested()
    {
        await using var serviceProvider = CreateOutboundOnlyServiceProvider();
        var publisher = serviceProvider.GetRequiredService<IMessagePublisher>();
        var target = serviceProvider.GetRequiredService<Topology>().GetRequiredTarget<OrderPlaced>();
        SerializedMessage message = new (
            "raw-body"u8.ToArray(),
            "application/octet-stream",
            null,
            new Dictionary<string, string?>(StringComparer.Ordinal),
            "raw-message-id",
            "raw-correlation-id"
        );
        using CancellationTokenSource cancellation = new ();
        await cancellation.CancelAsync();

        var act = async () => await publisher.PublishRawAsync(message, target, cancellation.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
        serviceProvider.GetRequiredService<InMemoryBroker>().GetMessages("orders").Should().BeEmpty();
    }

    [Fact]
    public async Task PublishMessageAsync_ThrowsAndRecordsNothingWhenCancellationIsAlreadyRequested()
    {
        CloudEventEnvelope envelope = new (
            "1.0",
            "93f0208d-10fe-47fc-a3e4-daed821f80b7",
            "/tests",
            "tests.order.placed",
            new DateTimeOffset(2026, 5, 31, 12, 34, 56, TimeSpan.Zero),
            null,
            "application/json",
            null,
            "custom-body"u8.ToArray(),
            new Dictionary<string, string?>(StringComparer.Ordinal)
        );
        await using var serviceProvider =
            CreateOutboundOnlyServiceProvider(new FixedEnvelopeMessageSerializer(envelope));
        var publisher = serviceProvider.GetRequiredService<IMessagePublisher>();
        var target = serviceProvider.GetRequiredService<Topology>().GetRequiredTarget<OrderPlaced>();
        using CancellationTokenSource cancellation = new ();
        await cancellation.CancelAsync();

        var act = async () => await publisher.PublishMessageAsync(
            new OrderPlaced { OrderId = "order-1" },
            target,
            cancellationToken: cancellation.Token
        );

        await act.Should().ThrowAsync<OperationCanceledException>();
        serviceProvider.GetRequiredService<InMemoryBroker>().GetMessages("orders").Should().BeEmpty();
    }

    [Fact]
    public async Task PublishMessageAsync_BindsCloudEventOptionalAttributesToPrefixedHeaders()
    {
        CloudEventEnvelope envelope = new (
            "1.0",
            "93f0208d-10fe-47fc-a3e4-daed821f80b7",
            "/tests/in-memory/custom-source",
            "tests.order.placed.custom",
            new DateTimeOffset(2026, 5, 31, 12, 34, 56, TimeSpan.Zero),
            "orders/order-42",
            "application/vnd.brilliantmessaging.order+json",
            "https://schemas.example.test/orders/placed.json",
            "custom-body"u8.ToArray(),
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                ["partitionkey"] = "order-42",
                ["emptyextension"] = null
            }
        );
        await using var serviceProvider =
            CreateOutboundOnlyServiceProvider(new FixedEnvelopeMessageSerializer(envelope));
        var publisher = serviceProvider.GetRequiredService<IMessagePublisher>();
        var target = serviceProvider.GetRequiredService<Topology>().GetRequiredTarget<OrderPlaced>();

        await publisher.PublishMessageAsync(
            new OrderPlaced { OrderId = "order-42" },
            target,
            cancellationToken: CancellationToken
        );

        var recorded = serviceProvider.GetRequiredService<InMemoryBroker>()
           .GetMessages("orders")
           .Should()
           .ContainSingle()
           .Which;
        var prefix = InMemoryOutboundTarget<OrderPlaced>.CloudEventsHeaderPrefix;
        recorded.Body.ToArray().Should().Equal("custom-body"u8.ToArray());
        recorded.ContentType.Should().Be("application/vnd.brilliantmessaging.order+json");
        recorded.MessageId.Should().Be(envelope.Id);
        recorded.Headers.Should().ContainKey($"{prefix}id").WhoseValue.Should().Be(envelope.Id);
        recorded.Headers.Should().ContainKey($"{prefix}specversion").WhoseValue.Should().Be("1.0");
        recorded.Headers.Should().ContainKey($"{prefix}source").WhoseValue.Should().Be(envelope.Source);
        recorded.Headers.Should().ContainKey($"{prefix}type").WhoseValue.Should().Be(envelope.Type);
        recorded.Headers.Should().ContainKey($"{prefix}time").WhoseValue.Should()
           .Be("2026-05-31T12:34:56.0000000+00:00");
        recorded.Headers.Should().ContainKey($"{prefix}subject").WhoseValue.Should().Be(envelope.Subject);
        recorded.Headers.Should().ContainKey($"{prefix}dataschema").WhoseValue.Should().Be(envelope.DataSchema);
        recorded.Headers.Should().ContainKey($"{prefix}partitionkey").WhoseValue.Should().Be("order-42");
        recorded.Headers.Should().ContainKey($"{prefix}emptyextension").WhoseValue.Should().BeNull();
    }

    [Fact]
    public async Task PrePipelineDeliveryMissingCloudEventsType_IsDeadLetteredWithoutInvokingHandler()
    {
        await using var host = await CreateDeadLetteringHostAsync(WithContracts);
        var message = CreateRawMessage(
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                ["cloudEvents:specversion"] = "1.0"
            }
        );

        await host.Publisher.PublishRawAsync(
            message,
            host.Topology.GetRequiredTarget<OrderPlaced>(),
            CancellationToken
        );
        await host.Broker.DrainUntilIdleAsync(Timeout, CancellationToken);

        host.Probe.Invocations.Should().BeEmpty();
        host.Broker.GetMessages("orders").Should().ContainSingle();
        var deadLetter = host.Broker.GetMessages("orders.dead").Should().ContainSingle().Which;
        deadLetter.Body.ToArray().Should().Equal(message.Body);
        deadLetter.Headers.Should().NotContainKey("cloudEvents:type");
    }

    [Fact]
    public async Task PrePipelineDeliveryWithUnregisteredCloudEventsType_IsDeadLetteredWithoutInvokingHandler()
    {
        await using var host = await CreateDeadLetteringHostAsync(WithContracts);
        var message = CreateRawMessage(
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                ["cloudEvents:type"] = "tests.order.unknown"
            }
        );

        await host.Publisher.PublishRawAsync(
            message,
            host.Topology.GetRequiredTarget<OrderPlaced>(),
            CancellationToken
        );
        await host.Broker.DrainUntilIdleAsync(Timeout, CancellationToken);

        host.Probe.Invocations.Should().BeEmpty();
        var deadLetter = host.Broker.GetMessages("orders.dead").Should().ContainSingle().Which;
        deadLetter.Headers.Should().ContainKey("cloudEvents:type").WhoseValue.Should().Be("tests.order.unknown");
    }

    [Fact]
    public async Task PrePipelineDeliveryWithInboundAliasButNoEndpoint_IsDeadLetteredWithoutInvokingHandler()
    {
        await using var host = await CreateDeadLetteringHostAsync(WithContractsAndLegacyOrderPlacedAlias);
        var message = CreateRawMessage(
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                ["cloudEvents:specversion"] = "1.0",
                ["cloudEvents:id"] = "legacy-message",
                ["cloudEvents:source"] = "/tests/legacy",
                ["cloudEvents:type"] = "tests.order.placed.legacy",
                ["cloudEvents:time"] = "2026-06-29T12:00:00.0000000+00:00"
            }
        );

        await host.Publisher.PublishRawAsync(
            message,
            host.Topology.GetRequiredTarget<OrderPlaced>(),
            CancellationToken
        );
        await host.Broker.DrainUntilIdleAsync(Timeout, CancellationToken);

        host.Probe.Invocations.Should().BeEmpty();
        var deadLetter = host.Broker.GetMessages("orders.dead").Should().ContainSingle().Which;
        deadLetter.Headers.Should()
           .ContainKey("cloudEvents:type")
           .WhoseValue.Should()
           .Be("tests.order.placed.legacy");
    }

    [Fact]
    public async Task PrePipelineDeliveryWithMalformedCloudEventsEnvelope_IsDeadLetteredWithoutInvokingHandler()
    {
        await using var host = await CreateDeadLetteringHostAsync(WithContracts);
        var message = CreateRawMessage(
            new Dictionary<string, string?>(StringComparer.Ordinal)
            {
                ["cloudEvents:type"] = "tests.order.placed"
            }
        );

        await host.Publisher.PublishRawAsync(
            message,
            host.Topology.GetRequiredTarget<OrderPlaced>(),
            CancellationToken
        );
        await host.Broker.DrainUntilIdleAsync(Timeout, CancellationToken);

        host.Probe.Invocations.Should().BeEmpty();
        var deadLetter = host.Broker.GetMessages("orders.dead").Should().ContainSingle().Which;
        deadLetter.Headers.Should().ContainKey("cloudEvents:type").WhoseValue.Should().Be("tests.order.placed");
    }

    [Fact]
    public async Task PublishMessageAsync_DoesNotRunHandlersInline()
    {
        TaskCompletionSource<bool> gate = new (TaskCreationOptions.RunContinuationsAsynchronously);
        await using var host = await InMemoryTestHost
           .StartAsync(
                builder => WithContracts(builder)
                   .AddInMemoryTopology(
                        topology => topology
                           .Topic("orders")
                           .Publish<OrderPlaced>(target => target.ToTopic("orders"))
                           .Consume("orders", consumer => consumer.Handle<OrderPlaced, OrderPlacedHandler>())
                    )
            );
        host.Probe.Gate = gate;

        await host.Publisher
           .PublishMessageAsync(new OrderPlaced { OrderId = "blocked" }, cancellationToken: CancellationToken)
           .WaitAsync(Timeout, CancellationToken);
        await host.Probe.WaitForInvocationsAsync(1, Timeout);

        var blockedDrain = async () => await host.Broker
           .DrainUntilIdleAsync(TimeSpan.FromMilliseconds(25), CancellationToken);

        await blockedDrain.Should().ThrowAsync<TimeoutException>();

        gate.SetResult(true);
        await host.Broker.DrainUntilIdleAsync(Timeout, CancellationToken);
    }

    [Fact]
    public async Task PublishMessageAsync_FansOutToEveryConsumerRouteForTopic()
    {
        await using var host = await InMemoryTestHost
           .StartAsync(
                builder => WithContracts(builder)
                   .AddInMemoryTopology(
                        topology => topology
                           .Topic("orders")
                           .Publish<OrderPlaced>(target => target.ToTopic("orders"))
                           .Consume("orders", consumer => consumer.Handle<OrderPlaced, OrderPlacedHandler>())
                           .Consume("orders", consumer => consumer.Handle<OrderPlaced, SecondOrderPlacedHandler>())
                    )
            );

        await host.Publisher
           .PublishMessageAsync(new OrderPlaced { OrderId = "fanout" }, cancellationToken: CancellationToken);
        await host.Broker.DrainUntilIdleAsync(Timeout, CancellationToken);

        host.Probe.Invocations
           .Select(static invocation => invocation.Message)
           .Should()
           .AllBeOfType<OrderPlaced>()
           .And
           .HaveCount(2);
        host.Broker.GetMessages("orders").Should().ContainSingle();
    }

    [Fact]
    public async Task Consume_DefaultConcurrencyPreservesFifoDelivery()
    {
        await using var host = await InMemoryTestHost
           .StartAsync(
                builder => WithContracts(builder)
                   .AddInMemoryTopology(
                        topology => topology
                           .Topic("orders")
                           .Publish<OrderPlaced>(target => target.ToTopic("orders"))
                           .Consume("orders", consumer => consumer.Handle<OrderPlaced, OrderPlacedHandler>())
                    )
            );

        await host.Publisher.PublishMessageAsync(
            new OrderPlaced { OrderId = "first" },
            cancellationToken: CancellationToken
        );
        await host.Publisher.PublishMessageAsync(
            new OrderPlaced { OrderId = "second" },
            cancellationToken: CancellationToken
        );
        await host.Publisher.PublishMessageAsync(
            new OrderPlaced { OrderId = "third" },
            cancellationToken: CancellationToken
        );
        await host.Broker.DrainUntilIdleAsync(Timeout, CancellationToken);

        host.Probe.Invocations
           .Select(static invocation => ((OrderPlaced) invocation.Message).OrderId)
           .Should()
           .Equal("first", "second", "third");
    }

    [Fact]
    public async Task Consume_ConfiguredConcurrencyProcessesDeliveriesInParallel()
    {
        TaskCompletionSource<bool> gate = new (TaskCreationOptions.RunContinuationsAsynchronously);
        await using var host = await InMemoryTestHost
           .StartAsync(
                builder => WithContracts(builder)
                   .AddInMemoryTopology(
                        topology => topology
                           .Topic("orders")
                           .Publish<OrderPlaced>(target => target.ToTopic("orders"))
                           .Consume(
                                "orders",
                                consumer => consumer
                                   .Concurrency(2)
                                   .Handle<OrderPlaced, OrderPlacedHandler>()
                            )
                    )
            );
        host.Probe.Gate = gate;

        await host.Publisher.PublishMessageAsync(
            new OrderPlaced { OrderId = "first" },
            cancellationToken: CancellationToken
        );
        await host.Publisher.PublishMessageAsync(
            new OrderPlaced { OrderId = "second" },
            cancellationToken: CancellationToken
        );
        await host.Probe.WaitForInvocationsAsync(2, Timeout);

        host.Probe.Invocations.Should().HaveCount(2);
        gate.SetResult(true);
        await host.Broker.DrainUntilIdleAsync(Timeout, CancellationToken);
    }

    [Fact]
    public async Task OnFailure_RetriesWithConfiguredBackoffUntilHandlerSucceeds()
    {
        ManualDelayScheduler scheduler = new ();
        await using var host = await InMemoryTestHost
           .StartAsync(
                builder => WithContracts(builder)
                   .AddInMemoryTopology(
                        topology => topology
                           .Topic("orders")
                           .Publish<OrderPlaced>(target => target.ToTopic("orders"))
                           .Consume(
                                "orders",
                                consumer => consumer
                                   .OnFailure(
                                        failure => failure.Retry(
                                            retry => retry
                                               .MaxAttempts(3)
                                               .LinearBackoff(TimeSpan.FromMilliseconds(10))
                                        )
                                    )
                                   .Handle<OrderPlaced, OrderPlacedHandler>()
                            )
                    ),
                scheduler
            );
        host.Probe.OnHandle = invocation => invocation.DeliveryAttempt < 3 ?
            new RetryMessageException("try again") :
            null;

        await host.Publisher.PublishMessageAsync(
            new OrderPlaced { OrderId = "retry" },
            cancellationToken: CancellationToken
        );
        await scheduler.WaitForPendingAsync(1, Timeout);
        scheduler.ReleaseAll();
        await scheduler.WaitForPendingAsync(1, Timeout);
        scheduler.ReleaseAll();
        await host.Broker.DrainUntilIdleAsync(Timeout, CancellationToken);

        host.Probe.Invocations.Select(static invocation => invocation.DeliveryAttempt).Should().Equal(1, 2, 3);
        scheduler.RequestedDelays.Should().Equal(TimeSpan.FromMilliseconds(10), TimeSpan.FromMilliseconds(20));
    }

    [Fact]
    public async Task DrainUntilIdleAsync_WaitsForAlreadyScheduledRetries()
    {
        ManualDelayScheduler scheduler = new ();
        await using var host = await InMemoryTestHost
           .StartAsync(
                builder => WithContracts(builder)
                   .AddInMemoryTopology(
                        topology => topology
                           .Topic("orders")
                           .Publish<OrderPlaced>(target => target.ToTopic("orders"))
                           .Consume(
                                "orders",
                                consumer => consumer
                                   .OnFailure(failure => failure.Retry(static retry => retry.MaxAttempts(2)))
                                   .Handle<OrderPlaced, OrderPlacedHandler>()
                            )
                    ),
                scheduler
            );
        host.Probe.OnHandle = invocation => invocation.DeliveryAttempt == 1 ?
            new RetryMessageException("retry once") :
            null;

        await host.Publisher.PublishMessageAsync(
            new OrderPlaced { OrderId = "drain" },
            cancellationToken: CancellationToken
        );
        await scheduler.WaitForPendingAsync(1, Timeout);

        var drain = host.Broker.DrainUntilIdleAsync(Timeout, CancellationToken);

        drain.IsCompleted.Should().BeFalse();
        scheduler.ReleaseAll();
        await drain;
        host.Probe.Invocations.Select(static invocation => invocation.DeliveryAttempt).Should().Equal(1, 2);
    }

    [Fact]
    public async Task OnFailure_DeadLettersWhenRetryAttemptsAreExhausted()
    {
        ManualDelayScheduler scheduler = new ();
        await using var host = await InMemoryTestHost
           .StartAsync(
                builder => WithContracts(builder)
                   .AddInMemoryTopology(
                        topology => topology
                           .Topic("orders")
                           .Topic("orders.dead")
                           .Publish<OrderPlaced>(target => target.ToTopic("orders"))
                           .Consume(
                                "orders",
                                consumer => consumer
                                   .OnFailure(
                                        failure => failure
                                           .Retry(static retry => retry.MaxAttempts(2))
                                           .DeadLetterTo("orders.dead")
                                    )
                                   .Handle<OrderPlaced, OrderPlacedHandler>()
                            )
                    ),
                scheduler
            );
        host.Probe.OnHandle = static _ => new InvalidOperationException("still failing");

        await host.Publisher.PublishMessageAsync(
            new OrderPlaced { OrderId = "dead" },
            cancellationToken: CancellationToken
        );
        await scheduler.WaitForPendingAsync(1, Timeout);
        scheduler.ReleaseAll();
        await host.Broker.DrainUntilIdleAsync(Timeout, CancellationToken);

        host.Probe.Invocations.Select(static invocation => invocation.DeliveryAttempt).Should().Equal(1, 2);
        host.Broker.GetMessages("orders.dead").Should().ContainSingle().Which.Topic.Should().Be("orders.dead");
    }

    [Fact]
    public async Task RetryMessageException_DropsWhenNoRetryPolicyIsConfigured()
    {
        ManualDelayScheduler scheduler = new ();
        await using var host = await InMemoryTestHost
           .StartAsync(
                builder => WithContracts(builder)
                   .AddInMemoryTopology(
                        topology => topology
                           .Topic("orders")
                           .Publish<OrderPlaced>(target => target.ToTopic("orders"))
                           .Consume("orders", consumer => consumer.Handle<OrderPlaced, OrderPlacedHandler>())
                    ),
                scheduler
            );
        host.Probe.OnHandle = static _ => new RetryMessageException("no retry policy");

        await host.Publisher.PublishMessageAsync(
            new OrderPlaced { OrderId = "drop" },
            cancellationToken: CancellationToken
        );
        await host.Broker.DrainUntilIdleAsync(Timeout, CancellationToken);

        host.Probe.Invocations.Should().ContainSingle().Which.DeliveryAttempt.Should().Be(1);
        scheduler.PendingCount.Should().Be(0);
    }

    [Fact]
    public async Task RejectMessageException_DeadLettersWithoutRetrying()
    {
        ManualDelayScheduler scheduler = new ();
        await using var host = await InMemoryTestHost
           .StartAsync(
                builder => WithContracts(builder)
                   .AddInMemoryTopology(
                        topology => topology
                           .Topic("orders")
                           .Topic("orders.dead")
                           .Publish<OrderPlaced>(target => target.ToTopic("orders"))
                           .Consume(
                                "orders",
                                consumer => consumer
                                   .OnFailure(
                                        failure => failure
                                           .Retry(static retry => retry.MaxAttempts(3))
                                           .DeadLetterTo("orders.dead")
                                    )
                                   .Handle<OrderPlaced, OrderPlacedHandler>()
                            )
                    ),
                scheduler
            );
        host.Probe.OnHandle = static _ => new RejectMessageException("poison");

        await host.Publisher.PublishMessageAsync(
            new OrderPlaced { OrderId = "poison" },
            cancellationToken: CancellationToken
        );
        await host.Broker.DrainUntilIdleAsync(Timeout, CancellationToken);

        host.Probe.Invocations.Should().ContainSingle().Which.DeliveryAttempt.Should().Be(1);
        scheduler.PendingCount.Should().Be(0);
        host.Broker.GetMessages("orders.dead").Should().ContainSingle();
    }

    [Fact]
    public async Task ManualAck_IgnoresDuplicateSettlement()
    {
        await using var host = await InMemoryTestHost
           .StartAsync(
                builder => WithContracts(builder)
                   .AddInMemoryTopology(
                        topology => topology
                           .Topic("orders")
                           .Topic("orders.dead")
                           .Publish<OrderPlaced>(target => target.ToTopic("orders"))
                           .Consume(
                                "orders",
                                consumer => consumer
                                   .OnFailure(failure => failure.DeadLetterTo("orders.dead"))
                                   .Handle<OrderPlaced, DoubleAckOrderPlacedHandler>(
                                        handler => handler.ManualAck()
                                    )
                            )
                    )
            );

        await host.Publisher.PublishMessageAsync(
            new OrderPlaced { OrderId = "double-ack" },
            cancellationToken: CancellationToken
        );
        await host.Broker.DrainUntilIdleAsync(Timeout, CancellationToken);

        host.Probe.Invocations.Should().ContainSingle();
        host.Broker.GetMessages("orders.dead").Should().BeEmpty();
    }

    [Fact]
    public async Task ManualNack_IgnoresDuplicateSettlement()
    {
        await using var host = await InMemoryTestHost
           .StartAsync(
                builder => WithContracts(builder)
                   .AddInMemoryTopology(
                        topology => topology
                           .Topic("orders")
                           .Topic("orders.dead")
                           .Publish<OrderPlaced>(target => target.ToTopic("orders"))
                           .Consume(
                                "orders",
                                consumer => consumer
                                   .OnFailure(failure => failure.DeadLetterTo("orders.dead"))
                                   .Handle<OrderPlaced, DoubleNackOrderPlacedHandler>(
                                        handler => handler.ManualAck()
                                    )
                            )
                    )
            );

        await host.Publisher.PublishMessageAsync(
            new OrderPlaced { OrderId = "double-nack" },
            cancellationToken: CancellationToken
        );
        await host.Broker.DrainUntilIdleAsync(Timeout, CancellationToken);

        host.Probe.Invocations.Should().ContainSingle();
        host.Broker.GetMessages("orders.dead").Should().ContainSingle();
    }

    [Fact]
    public async Task Brokers_AreIsolatedPerServiceProviderByDefault()
    {
        await using var first = await CreateOutboundOnlyHostAsync();
        await using var second = await CreateOutboundOnlyHostAsync();

        await first.Publisher.PublishMessageAsync(
            new OrderPlaced { OrderId = "first" },
            cancellationToken: CancellationToken
        );
        await second.Publisher.PublishMessageAsync(
            new OrderPlaced { OrderId = "second" },
            cancellationToken: CancellationToken
        );

        first.Broker.Should().NotBeSameAs(second.Broker);
        first.Broker.GetMessages("orders").Should().ContainSingle();
        second.Broker.GetMessages("orders").Should().ContainSingle();
    }

    [Fact]
    public async Task DirectionSpecificDefaultTopologies_RegisterIsolatedBrokers()
    {
        await using var host = await InMemoryTestHost
           .StartAsync(
                builder => WithContracts(builder)
                   .AddInMemoryOutboundTopology(
                        topology => topology
                           .Topic("orders")
                           .Publish<OrderPlaced>(target => target.ToTopic("orders"))
                    )
                   .AddInMemoryInboundTopology(
                        topology => topology
                           .Topic("orders")
                           .Consume("orders", consumer => consumer.Handle<OrderPlaced, OrderPlacedHandler>())
                    )
            );

        var outboundBroker = host.Broker;
        var inboundBroker = host.BrokerFor(InMemoryTransportModule.DefaultInboundName);

        outboundBroker.Should().NotBeSameAs(inboundBroker);

        await host.Publisher.PublishMessageAsync(
            new OrderPlaced { OrderId = "split-default" },
            cancellationToken: CancellationToken
        );
        await outboundBroker.DrainUntilIdleAsync(Timeout, CancellationToken);
        await inboundBroker.DrainUntilIdleAsync(Timeout, CancellationToken);

        outboundBroker.GetMessages("orders").Should().ContainSingle();
        inboundBroker.GetMessages("orders").Should().BeEmpty();
        host.Probe.Invocations.Should().BeEmpty();
    }

    [Fact]
    public async Task DirectionSpecificNamedTopologies_RegisterKeyedBrokers()
    {
        await using var host = await InMemoryTestHost
           .StartAsync(
                builder => WithContracts(builder)
                   .AddInMemoryOutboundTopology(
                        "outbound",
                        topology => topology
                           .Topic("orders")
                           .Publish<OrderPlaced>(target => target.ToTopic("orders"))
                    )
                   .AddInMemoryInboundTopology(
                        "inbound",
                        topology => topology
                           .Topic("orders")
                           .Consume("orders", consumer => consumer.Handle<OrderPlaced, OrderPlacedHandler>())
                    )
            );

        var outboundBroker = host.BrokerFor("outbound");
        var inboundBroker = host.BrokerFor("inbound");

        outboundBroker.Should().NotBeSameAs(inboundBroker);

        await host.Publisher
           .ForTopology("outbound")
           .PublishMessageAsync(new OrderPlaced { OrderId = "split-named" }, cancellationToken: CancellationToken);
        await outboundBroker.DrainUntilIdleAsync(Timeout, CancellationToken);
        await inboundBroker.DrainUntilIdleAsync(Timeout, CancellationToken);

        outboundBroker.GetMessages("orders").Should().ContainSingle();
        inboundBroker.GetMessages("orders").Should().BeEmpty();
        host.Probe.Invocations.Should().BeEmpty();
    }

    [Fact]
    public async Task StopAsync_StopsAcceptingPublishedWork()
    {
        await using var host = await InMemoryTestHost
           .StartAsync(
                builder => WithContracts(builder)
                   .AddInMemoryTopology(
                        topology => topology
                           .Topic("orders")
                           .Publish<OrderPlaced>(target => target.ToTopic("orders"))
                           .Consume("orders", consumer => consumer.Handle<OrderPlaced, OrderPlacedHandler>())
                    )
            );
        await host.StopRuntimesAsync(CancellationToken);

        var act = async () => await host.Publisher
           .PublishMessageAsync(new OrderPlaced { OrderId = "after-stop" }, cancellationToken: CancellationToken);

        await act.Should().ThrowAsync<InvalidOperationException>();
    }

    [Fact]
    public async Task StopAsync_CancelsScheduledRetriesAfterShutdownTimeout()
    {
        ManualDelayScheduler scheduler = new ();
        await using var host = await InMemoryTestHost
           .StartAsync(
                builder => WithContracts(builder)
                   .AddInMemoryTopology(
                        topology => topology
                           .ShutdownTimeout(TimeSpan.FromMilliseconds(25))
                           .Topic("orders")
                           .Publish<OrderPlaced>(target => target.ToTopic("orders"))
                           .Consume(
                                "orders",
                                consumer => consumer
                                   .OnFailure(failure => failure.Retry(static retry => retry.MaxAttempts(2)))
                                   .Handle<OrderPlaced, OrderPlacedHandler>()
                            )
                    ),
                scheduler
            );
        host.Probe.OnHandle = static _ => new RetryMessageException("scheduled retry");

        await host.Publisher.PublishMessageAsync(
            new OrderPlaced { OrderId = "shutdown" },
            cancellationToken: CancellationToken
        );
        await scheduler.WaitForPendingAsync(1, Timeout);

        await host.StopRuntimesAsync(CancellationToken).WaitAsync(Timeout, CancellationToken);
        await host.Broker.DrainUntilIdleAsync(Timeout, CancellationToken);
    }

    [Fact]
    public async Task StopAsync_WhenCallerCancellationInterruptsDrain_CancelsWorkersAndPropagatesCancellation()
    {
        TaskCompletionSource<bool> gate = new (TaskCreationOptions.RunContinuationsAsynchronously);
        await using var host = await InMemoryTestHost
           .StartAsync(
                builder => WithContracts(builder)
                   .AddInMemoryTopology(
                        topology => topology
                           .Topic("orders")
                           .Publish<OrderPlaced>(target => target.ToTopic("orders"))
                           .Consume("orders", consumer => consumer.Handle<OrderPlaced, OrderPlacedHandler>())
                    )
            );
        host.Probe.Gate = gate;

        await host.Publisher.PublishMessageAsync(
            new OrderPlaced { OrderId = "cancel-stop" },
            cancellationToken: CancellationToken
        );
        await host.Probe.WaitForInvocationsAsync(1, Timeout);
        using CancellationTokenSource cancellation = new ();
        await cancellation.CancelAsync();

        var act = async () => await host.StopRuntimesAsync(cancellation.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
        var publishAfterStop = async () => await host.Publisher.PublishMessageAsync(
            new OrderPlaced { OrderId = "after-cancelled-stop" },
            cancellationToken: CancellationToken
        );
        await publishAfterStop.Should().ThrowAsync<InvalidOperationException>();
    }

    [Fact]
    public void Compile_RejectsReferencesToUndeclaredTopics()
    {
        var services = new ServiceCollection();
        WithContracts(services.AddBrilliantMessaging().UseCloudEvents(static options => options.Source = "/tests"))
           .AddInMemoryTopology(
                topology => topology
                   .Topic("declared")
                   .Publish<OrderPlaced>(target => target.ToTopic("missing-publish"))
                   .Consume("missing-consume", consumer => consumer.Handle<OrderPlaced, OrderPlacedHandler>())
            );
        using var serviceProvider = services.BuildServiceProvider();

        var act = () => _ = serviceProvider.GetRequiredService<Topology>();

        var exception = act.Should().Throw<TopologyValidationException>().Which;
        exception.ValidationErrors.Should().Contain(
            $"Outbound target for message '{typeof(OrderPlaced).FullName}' publishes to undeclared topic 'missing-publish'. Declare it with Topic(\"missing-publish\")."
        );
        exception.ValidationErrors.Should().Contain(
            "Consumer for topic 'missing-consume' references an undeclared topic. Declare it with Topic(\"missing-consume\")."
        );
    }

    private static Task<InMemoryTestHost> CreateOutboundOnlyHostAsync()
    {
        return InMemoryTestHost.StartAsync(
            builder => WithContracts(builder)
               .AddInMemoryTopology(
                    topology => topology
                       .Topic("orders")
                       .Publish<OrderPlaced>(target => target.ToTopic("orders"))
                )
        );
    }

    private static Task<InMemoryTestHost> CreateDeadLetteringHostAsync(
        Func<BrilliantMessagingBuilder, BrilliantMessagingBuilder> configureContracts
    )
    {
        return InMemoryTestHost.StartAsync(
            builder => configureContracts(builder)
               .AddInMemoryTopology(
                    topology => topology
                       .Topic("orders")
                       .Topic("orders.dead")
                       .Publish<OrderPlaced>(target => target.ToTopic("orders"))
                       .Consume(
                            "orders",
                            consumer => consumer
                               .OnFailure(failure => failure.DeadLetterTo("orders.dead"))
                               .Handle<OrderPlaced, OrderPlacedHandler>()
                        )
                )
        );
    }

    private static SerializedMessage CreateRawMessage(IReadOnlyDictionary<string, string?> headers)
    {
        return new SerializedMessage(
            "{}"u8.ToArray(),
            "application/json",
            null,
            headers,
            "raw-message",
            null
        );
    }

    private static ServiceProvider CreateOutboundOnlyServiceProvider(FixedEnvelopeMessageSerializer? serializer = null)
    {
        ServiceCollection services = new ();
        if (serializer is not null)
        {
            services.AddSingleton(serializer);
        }

        WithContracts(services.AddBrilliantMessaging().UseCloudEvents(static options => options.Source = "/tests"))
           .AddInMemoryTopology(
                topology =>
                {
                    topology.Topic("orders");
                    topology.Publish<OrderPlaced>(
                        target =>
                        {
                            target.ToTopic("orders");
                            if (serializer is not null)
                            {
                                target.WithSerializer<FixedEnvelopeMessageSerializer>();
                            }
                        }
                    );
                }
            );

        return services.BuildServiceProvider();
    }

    private static BrilliantMessagingBuilder WithContractsAndLegacyOrderPlacedAlias(BrilliantMessagingBuilder builder)
    {
        return builder.MapMessageContracts(
            static contracts =>
            {
                contracts.Map<OrderPlaced>("tests.order.placed").WithInboundAlias("tests.order.placed.legacy");
                contracts.Map<OrderShipped>("tests.order.shipped");
            }
        );
    }

    private static BrilliantMessagingBuilder WithContracts(BrilliantMessagingBuilder builder)
    {
        return builder.MapMessageContracts(
            static contracts =>
            {
                contracts.Map<OrderPlaced>("tests.order.placed");
                contracts.Map<OrderShipped>("tests.order.shipped");
            }
        );
    }
}

public sealed class FixedEnvelopeMessageSerializer : IMessageSerializer
{
    private readonly CloudEventEnvelope _envelope;

    public FixedEnvelopeMessageSerializer(CloudEventEnvelope envelope)
    {
        _envelope = envelope;
    }

    public ValueTask<CloudEventEnvelope> SerializeAsync<T>(
        T message,
        in CloudEventMetadata metadata,
        string? type,
        string? dataSchema,
        CancellationToken cancellationToken = default
    )
    {
        return new ValueTask<CloudEventEnvelope>(_envelope);
    }
}
