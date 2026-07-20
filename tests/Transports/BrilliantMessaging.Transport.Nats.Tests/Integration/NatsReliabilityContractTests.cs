using System;
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
using Xunit;

namespace BrilliantMessaging.Transport.Nats.Tests.Integration;

/// <summary>
/// Documents the JetStream reliability contract of the NATS transport: publish acknowledgements must surface
/// server-side rejections, dead-letter routing must only settle the original after a successful republish, and
/// consumption must survive transient JetStream failures.
/// </summary>
[Collection<NatsCollection>]
public sealed class NatsReliabilityContractTests : IAsyncLifetime
{
    private readonly NatsFixture _fixture;

    public NatsReliabilityContractTests(NatsFixture fixture)
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
    public async Task PublishRejectedByStreamSurfacesAsError()
    {
        ServiceCollection services = new ();
        services
           .AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology => topology
                   .UseServer(_fixture.ConnectionString)
                   .Stream("LIMITED", stream => stream.Subject("limited.orders").MaxMessageSize(8))
                   .Publish<OrderPlaced>(target => target.ToSubject("limited.orders"))
            );
        await using var provider = services.BuildServiceProvider();

        foreach (var provisioner in provider.GetServices<ITopologyProvisioner>())
        {
            await provisioner.ProvisionAsync(TestContext.Current.CancellationToken);
        }

        var publisher = provider.GetRequiredService<IMessagePublisher>();

        // The stream's MaxMessageSize rejects the publish, so the JetStream PubAck carries an error instead of
        // a stream sequence. Awaiting the acknowledgement is only meaningful when that rejection surfaces as an
        // exception; otherwise the message is lost while the publish reports success.
        var act = async () => await publisher.PublishMessageAsync(
            new OrderPlaced { OrderId = "order-too-large" },
            cancellationToken: TestContext.Current.CancellationToken
        );

        await act
           .Should().ThrowAsync<NatsJSApiException>(
                "a JetStream publish rejected by the server must surface its API error"
            );
    }

    [Fact]
    public async Task FailedDeadLetterPublishDoesNotTerminateOriginalDelivery()
    {
        PoisonProbe probe = new ();
        ServiceCollection services = new ();
        services.AddSingleton(probe);
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology => topology
                   .UseServer(_fixture.ConnectionString)
                   .Stream("ORDERS", stream => stream.Subject("orders.*"))
                   .Stream("DEAD", stream => stream.Subject("dead.orders").MaxMessageSize(8))
                   .Publish<OrderPlaced>(target => target.ToSubject("orders.poison"))
                   .Consume(
                        "ORDERS",
                        "orders-poison-worker",
                        consumer => consumer
                           .FilterSubject("orders.poison")
                           .MaxDeliver(6)
                           .DeadLetterAfterDeliveryAttempt(3)
                           .AckWait(TimeSpan.FromSeconds(3))
                           .DeadLetterSubject("dead.orders")
                           .Handle<OrderPlaced, PoisonOrderPlacedHandler>()
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
                new OrderPlaced { OrderId = "order-poison" },
                cancellationToken: TestContext.Current.CancellationToken
            );

            // The DEAD stream's MaxMessageSize rejects every dead-letter republish, so the JetStream PubAck for
            // the dead-letter publish carries an error. Dead-letter routing must only settle the original after
            // the republish is acknowledged as successful; terminating it anyway loses the message. With the
            // original left unsettled, AckWait redelivers it, so a second delivery attempt must be observed.
            var attempt = await probe.WaitForRedeliveryAsync(
                TimeSpan.FromSeconds(15),
                TestContext.Current.CancellationToken
            );
            attempt.Should().BeGreaterThan(1);
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
    public async Task ConsumptionRecoversAfterDurableConsumerIsRecreated()
    {
        RecoveryProbe probe = new ();
        ServiceCollection services = new ();
        services.AddSingleton(probe);
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("tests.order.placed"))
           .AddNatsTopology(
                topology => topology
                   .UseServer(_fixture.ConnectionString)
                   .Stream("ORDERS", stream => stream.Subject("orders.*"))
                   .Publish<OrderPlaced>(target => target.ToSubject("orders.recovery"))
                   .Consume(
                        "ORDERS",
                        "orders-recovery-worker",
                        consumer => consumer
                           .FilterSubject("orders.recovery")
                           .Handle<OrderPlaced, RecoveryOrderPlacedHandler>()
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
                new OrderPlaced { OrderId = "order-before-outage" },
                cancellationToken: TestContext.Current.CancellationToken
            );
            await probe.WaitForInitialAsync(TimeSpan.FromSeconds(15), TestContext.Current.CancellationToken);

            // Deleting the durable consumer terminates the pending server pull with a 409 "Consumer Deleted";
            // re-provisioning afterwards restores the durable consumer with its original configuration. A
            // transient JetStream failure like this must not silently and permanently stop consumption while
            // the application keeps reporting healthy, so once the consumer exists again, messages must flow.
            await using NatsConnection connection = new (new NatsOpts { Url = _fixture.ConnectionString });
            NatsJSContext jetStream = new (connection);
            await jetStream.DeleteConsumerAsync(
                "ORDERS",
                "orders-recovery-worker",
                TestContext.Current.CancellationToken
            );
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);

            foreach (var provisioner in provider.GetServices<ITopologyProvisioner>())
            {
                await provisioner.ProvisionAsync(TestContext.Current.CancellationToken);
            }

            await publisher.PublishMessageAsync(
                new OrderPlaced { OrderId = "order-after-recovery" },
                cancellationToken: TestContext.Current.CancellationToken
            );

            var recovered = await probe.WaitForRecoveryAsync(
                TimeSpan.FromSeconds(15),
                TestContext.Current.CancellationToken
            );
            recovered.OrderId.Should().Be("order-after-recovery");
        }
        finally
        {
            foreach (var runtime in runtimes)
            {
                await runtime.StopAsync(CancellationToken.None);
            }
        }
    }

    private sealed class PoisonProbe
    {
        private readonly TaskCompletionSource<uint> _redelivered =
            new (TaskCreationOptions.RunContinuationsAsynchronously);

        public void Observe(IncomingMessageContext context)
        {
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
                    "Timed out waiting for the poison message to be redelivered after its dead-letter publish " +
                    "was rejected; the original delivery was most likely terminated despite the failed republish."
                );
            }

            return await _redelivered.Task.ConfigureAwait(false);
        }
    }

    private sealed class RecoveryProbe
    {
        private readonly TaskCompletionSource<OrderPlaced> _afterRecovery =
            new (TaskCreationOptions.RunContinuationsAsynchronously);

        private readonly TaskCompletionSource<OrderPlaced> _initial =
            new (TaskCreationOptions.RunContinuationsAsynchronously);

        public void Record(OrderPlaced message)
        {
            if (string.Equals(message.OrderId, "order-before-outage", StringComparison.Ordinal))
            {
                _initial.TrySetResult(message);
            }

            if (string.Equals(message.OrderId, "order-after-recovery", StringComparison.Ordinal))
            {
                _afterRecovery.TrySetResult(message);
            }
        }

        public async Task<OrderPlaced> WaitForInitialAsync(TimeSpan timeout, CancellationToken cancellationToken)
        {
            var delay = Task.Delay(timeout, cancellationToken);
            var completed = await Task.WhenAny(_initial.Task, delay).ConfigureAwait(false);
            if (completed == delay)
            {
                throw new TimeoutException("Timed out waiting for the pre-outage NATS message.");
            }

            return await _initial.Task.ConfigureAwait(false);
        }

        public async Task<OrderPlaced> WaitForRecoveryAsync(TimeSpan timeout, CancellationToken cancellationToken)
        {
            var delay = Task.Delay(timeout, cancellationToken);
            var completed = await Task.WhenAny(_afterRecovery.Task, delay).ConfigureAwait(false);
            if (completed == delay)
            {
                throw new TimeoutException(
                    "Timed out waiting for the post-recovery NATS message; the consume loop most likely died " +
                    "with the deleted durable consumer and was never re-established."
                );
            }

            return await _afterRecovery.Task.ConfigureAwait(false);
        }
    }

    private sealed class PoisonOrderPlacedHandler : IMessageHandler<OrderPlaced>
    {
        private readonly PoisonProbe _probe;

        public PoisonOrderPlacedHandler(PoisonProbe probe)
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
            throw new RejectMessageException("Simulated poison message.");
        }
    }

    private sealed class RecoveryOrderPlacedHandler : IMessageHandler<OrderPlaced>
    {
        private readonly RecoveryProbe _probe;

        public RecoveryOrderPlacedHandler(RecoveryProbe probe)
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
}
