using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Abstractions;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;
using BrilliantMessaging.Core.Messaging.Outbound;
using BrilliantMessaging.Transport.RabbitMq.Tests.TestSupport;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using Testcontainers.RabbitMq;
using Xunit;

namespace BrilliantMessaging.Transport.RabbitMq.Tests.Integration;

[Collection<RabbitMqCollection>]
public sealed class RabbitMqQueuePolicyIntegrationTests
{
    private readonly RabbitMqContainer _container;

    public RabbitMqQueuePolicyIntegrationTests(RabbitMqFixture fixture)
    {
        _container = fixture.Container;
    }

    [Fact]
    public async Task DeliveryLimitKnob_DeadLettersRetryableFailureAtConfiguredLimit()
    {
        const string sourceExchange = "policy-delivery-exchange";
        const string deadLetterExchange = "policy-delivery-dlx";
        const string retryQueue = "policy-delivery-retry-queue";
        const string deadLetterQueue = "policy-delivery-dead-letter-queue";
        const string discriminator = "tests.rabbitmq.policy.delivery";
        var cancellationToken = TestContext.Current.CancellationToken;
        {
            RedeliveryAttemptSink sink = new ();
            var services = new ServiceCollection();
            services.AddSingleton(sink);
            services
               .AddBrilliantMessaging()
               .UseCloudEvents(options => options.Source = "/tests/rabbitmq/policy")
               .MapMessageContracts(
                    contracts => contracts.Map<PolicyDeliveryMessage>(discriminator)
                )
               .AddRabbitMqTopology(
                    builder =>
                    {
                        builder.UseConnectionFactory(
                            _ => new ConnectionFactory
                            {
                                Uri = new Uri(_container.GetConnectionString())
                            }
                        );
                        builder.Exchange(sourceExchange, ExchangeType.Direct);
                        builder.Exchange(deadLetterExchange, ExchangeType.Direct);
                        builder.Queue(
                            retryQueue,
                            queue => queue
                               .WithDeliveryLimit(3)
                               .WithDeadLetterExchange(deadLetterExchange)
                               .WithDeadLetterRoutingKey("dead")
                        );
                        builder.Queue(deadLetterQueue);
                        builder.QueueBinding(sourceExchange, retryQueue, "retry");
                        builder.QueueBinding(deadLetterExchange, deadLetterQueue, "dead");
                        builder.Publish<PolicyDeliveryMessage>(
                            target => target
                               .ToDirectExchange(sourceExchange, "retry")
                               .WithSerializer<CloudEventMessageSerializer>()
                        );
                        builder.Consume(
                            retryQueue,
                            consumer => consumer.Handle<PolicyDeliveryMessage, ThrowingPolicyDeliveryHandler>()
                        );
                    }
                );

            await using var serviceProvider = services.BuildServiceProvider();
            var hostedServices = serviceProvider.GetServices<IHostedService>().ToArray();

            try
            {
                foreach (var hostedService in hostedServices)
                {
                    await hostedService.StartAsync(cancellationToken);
                }

                var publisher = serviceProvider.GetRequiredService<IMessagePublisher>();
                await publisher.PublishMessageAsync(
                    new PolicyDeliveryMessage("retry"),
                    cancellationToken: cancellationToken
                );

                await sink.WaitForRedeliveryAsync(cancellationToken);

                var connectionFactory = new ConnectionFactory
                {
                    Uri = new Uri(_container.GetConnectionString())
                };
                await using var connection = await connectionFactory.CreateConnectionAsync(cancellationToken);
                await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);

                var deadLetter = await GetRequiredMessageAsync(
                    channel,
                    deadLetterQueue,
                    cancellationToken,
                    maximumAttempts: 120
                );

                sink.Attempts.Should().HaveCountGreaterThan(1);
                Encoding.UTF8.GetString(deadLetter.Body.ToArray()).Should().Be("{\"Value\":\"retry\"}");
                GetDeathReason(deadLetter).Should().Be("delivery_limit");
            }
            finally
            {
                foreach (var hostedService in hostedServices.Reverse())
                {
                    await hostedService.StopAsync(CancellationToken.None);
                }
            }
        }
    }

    [Fact]
    public async Task IntroduceV2WithDeleteBinding_UnbindsOldQueueAndRoutesToNewQueue()
    {
        const string exchange = "policy-evolve-exchange";
        const string v1Queue = "policy-evolve-orders-v1";
        const string v2Queue = "policy-evolve-orders-v2";
        var cancellationToken = TestContext.Current.CancellationToken;
        {
            // Phase 1: provision the v1 topology with an Active binding ex → v1 and no consumer, then publish.
            // The message lands in v1, proving the binding routes traffic into v1 on the broker.
            var firstServices = new ServiceCollection();
            firstServices
               .AddTestCloudEvents()
               .AddRabbitMqTopology(
                    builder =>
                    {
                        builder.UseConnectionFactory(
                            _ => new ConnectionFactory
                            {
                                Uri = new Uri(_container.GetConnectionString())
                            }
                        );
                        builder.Exchange(exchange, ExchangeType.Direct);
                        builder.Queue(v1Queue);
                        builder.QueueBinding(exchange, v1Queue, "orders.created");
                        builder.Publish<RabbitMqPublishMessage>(
                            target => target
                               .ToDirectExchange(exchange, "orders.created")
                               .WithSerializer<CloudEventMessageSerializer>()
                        );
                    }
                );

            await using var firstServiceProvider = firstServices.BuildServiceProvider();
            var firstHostedServices = firstServiceProvider.GetServices<IHostedService>().ToArray();

            try
            {
                foreach (var hostedService in firstHostedServices)
                {
                    await hostedService.StartAsync(cancellationToken);
                }

                var firstPublisher = firstServiceProvider.GetRequiredService<IMessagePublisher>();
                await firstPublisher.PublishMessageAsync(
                    new RabbitMqPublishMessage(100, "created"),
                    cancellationToken: cancellationToken
                );

                var connectionFactory = new ConnectionFactory
                {
                    Uri = new Uri(_container.GetConnectionString())
                };
                await using var connection = await connectionFactory.CreateConnectionAsync(cancellationToken);
                await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);

                await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
                (await channel.MessageCountAsync(v1Queue, cancellationToken)).Should().BeGreaterThan(0);

                // Drain v1 so we can cleanly assert later that no new messages route to it after the unbind.
                var drainResult = await channel.BasicGetAsync(v1Queue, true, cancellationToken);
                drainResult.Should().NotBeNull();
                (await channel.MessageCountAsync(v1Queue, cancellationToken)).Should().Be(0);

                // Phase 2: re-provision with the v1 binding flipped to Delete and a new v2 binding Active.
                // The provisioner binds ex → v2 first (Active pass) and then unbinds the pre-existing
                // ex → v1 binding (Delete pass), proving a binding that existed on the broker is actually removed.
                ConsumedMessageSink sink = new ();
                var secondServices = new ServiceCollection();
                secondServices.AddSingleton(sink);
                secondServices
                   .AddTestCloudEvents()
                   .AddRabbitMqTopology(
                        builder =>
                        {
                            builder.UseConnectionFactory(
                                _ => new ConnectionFactory
                                {
                                    Uri = new Uri(_container.GetConnectionString())
                                }
                            );
                            builder.Exchange(exchange, ExchangeType.Direct);
                            builder.Queue(v1Queue);
                            builder.Queue(v2Queue);
                            // The old binding is flipped to Delete so the broker unbinds the existing ex → orders-v1.
                            builder.QueueBinding(
                                exchange,
                                v1Queue,
                                "orders.created",
                                binding => binding.WithBindingMode(RabbitMqBindingMode.Delete)
                            );
                            // The new binding is Active so the broker binds ex → orders-v2.
                            builder.QueueBinding(exchange, v2Queue, "orders.created");
                            builder.Publish<RabbitMqPublishMessage>(
                                target => target
                                   .ToDirectExchange(exchange, "orders.created")
                                   .WithSerializer<CloudEventMessageSerializer>()
                            );
                            builder.Consume(
                                v2Queue,
                                consumer => consumer
                                   .PrefetchCount(1)
                                   .Concurrency(1)
                                   .Handle<RabbitMqPublishMessage, RecordingPublishMessageHandler>()
                            );
                        }
                    );

                await using var secondServiceProvider = secondServices.BuildServiceProvider();
                var secondHostedServices = secondServiceProvider.GetServices<IHostedService>().ToArray();

                try
                {
                    foreach (var hostedService in secondHostedServices)
                    {
                        await hostedService.StartAsync(cancellationToken);
                    }

                    var secondPublisher = secondServiceProvider.GetRequiredService<IMessagePublisher>();
                    await secondPublisher.PublishMessageAsync(
                        new RabbitMqPublishMessage(200, "created"),
                        cancellationToken: cancellationToken
                    );

                    var consumed = await sink.WaitAsync(cancellationToken);
                    consumed.Id.Should().Be(200);

                    // The v1 queue should still have no messages — the unbind means new publishes no longer route to it.
                    (await channel.MessageCountAsync(v1Queue, cancellationToken)).Should().Be(0);
                }
                finally
                {
                    foreach (var hostedService in secondHostedServices.Reverse())
                    {
                        await hostedService.StopAsync(CancellationToken.None);
                    }
                }
            }
            finally
            {
                foreach (var hostedService in firstHostedServices.Reverse())
                {
                    await hostedService.StopAsync(CancellationToken.None);
                }
            }
        }
    }

    [Theory]
    [InlineData(RabbitMqQueueType.Classic)]
    [InlineData(RabbitMqQueueType.Quorum)]
    public async Task DeleteMode_RemovesEmptyQueueAndFailsWhenQueueHasMessages(RabbitMqQueueType queueType)
    {
        var exchange = $"policy-delete-exchange-{queueType}";
        var queueName = $"policy-delete-queue-{queueType}";
        var cancellationToken = TestContext.Current.CancellationToken;
        Action<RabbitMqQueueBuilder> applyQueueType = queueType == RabbitMqQueueType.Classic ?
            static queue => queue.AsClassicQueue() :
            static queue => queue.AsQuorumQueue();
        {
            // Step 1: declare the queue and publish a message to it (no consumer, so the message stays).
            var firstServices = new ServiceCollection();
            firstServices
               .AddTestCloudEvents()
               .AddRabbitMqTopology(
                    builder =>
                    {
                        builder.UseConnectionFactory(
                            _ => new ConnectionFactory
                            {
                                Uri = new Uri(_container.GetConnectionString())
                            }
                        );
                        builder.Exchange(exchange, ExchangeType.Direct);
                        builder.Queue(queueName, applyQueueType);
                        builder.QueueBinding(exchange, queueName, "delete.test");
                        builder.Publish<RabbitMqPublishMessage>(
                            target => target
                               .ToDirectExchange(exchange, "delete.test")
                               .WithSerializer<CloudEventMessageSerializer>()
                        );
                    }
                );

            await using var firstServiceProvider = firstServices.BuildServiceProvider();
            var firstHostedServices = firstServiceProvider.GetServices<IHostedService>().ToArray();

            try
            {
                foreach (var hostedService in firstHostedServices)
                {
                    await hostedService.StartAsync(cancellationToken);
                }

                var publisher = firstServiceProvider.GetRequiredService<IMessagePublisher>();
                await publisher.PublishMessageAsync(
                    new RabbitMqPublishMessage(200, "delete"),
                    cancellationToken: cancellationToken
                );

                var connectionFactory = new ConnectionFactory
                {
                    Uri = new Uri(_container.GetConnectionString())
                };
                await using var connection = await connectionFactory.CreateConnectionAsync(cancellationToken);
                await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);

                await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
                (await channel.MessageCountAsync(queueName, cancellationToken)).Should().BeGreaterThan(0);

                // Step 2: try to delete the queue while it has messages — should fail.
                var failingServices = new ServiceCollection();
                failingServices
                   .AddTestCloudEvents()
                   .AddRabbitMqTopology(
                        builder =>
                        {
                            builder.UseConnectionFactory(
                                _ => new ConnectionFactory
                                {
                                    Uri = new Uri(_container.GetConnectionString())
                                }
                            );
                            builder.Exchange(exchange, ExchangeType.Direct);
                            builder.Queue(
                                queueName,
                                queue => queue.WithDeclareMode(RabbitMqDeclareMode.Delete)
                            );
                        }
                    );

                await using var failingServiceProvider = failingServices.BuildServiceProvider();
                var failingHostedServices = failingServiceProvider.GetServices<IHostedService>().ToArray();

                var act = async () =>
                {
                    foreach (var hostedService in failingHostedServices)
                    {
                        await hostedService.StartAsync(cancellationToken);
                    }
                };

                await act.Should().ThrowAsync<Exception>();
                // The queue should still exist on the broker.
                (await channel.MessageCountAsync(queueName, cancellationToken)).Should().BeGreaterThan(0);

                // Step 3: drain the queue.
                var drainResult = await channel.BasicGetAsync(queueName, true, cancellationToken);
                drainResult.Should().NotBeNull();

                // Step 4: delete the queue now that it is empty — should succeed.
                var deletingServices = new ServiceCollection();
                deletingServices
                   .AddTestCloudEvents()
                   .AddRabbitMqTopology(
                        builder =>
                        {
                            builder.UseConnectionFactory(
                                _ => new ConnectionFactory
                                {
                                    Uri = new Uri(_container.GetConnectionString())
                                }
                            );
                            builder.Exchange(exchange, ExchangeType.Direct);
                            builder.Queue(
                                queueName,
                                queue => queue.WithDeclareMode(RabbitMqDeclareMode.Delete)
                            );
                        }
                    );

                await using var deletingServiceProvider = deletingServices.BuildServiceProvider();
                var deletingHostedServices = deletingServiceProvider.GetServices<IHostedService>().ToArray();

                foreach (var hostedService in deletingHostedServices)
                {
                    await hostedService.StartAsync(cancellationToken);
                }

                // The queue should be gone from the broker.
                await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
                await AssertQueueAbsentAsync(channel, queueName, cancellationToken);
            }
            finally
            {
                foreach (var hostedService in firstHostedServices.Reverse())
                {
                    await hostedService.StopAsync(CancellationToken.None);
                }
            }
        }
    }

    [Fact]
    public async Task DeleteMode_SucceedsWhenQueueAndBindingAlreadyAbsent()
    {
        const string exchange = "policy-idempotent-exchange";
        const string queueName = "policy-idempotent-absent-queue";
        var cancellationToken = TestContext.Current.CancellationToken;
        {
            var services = new ServiceCollection();
            services
               .AddTestCloudEvents()
               .AddRabbitMqTopology(
                    builder =>
                    {
                        builder.UseConnectionFactory(
                            _ => new ConnectionFactory
                            {
                                Uri = new Uri(_container.GetConnectionString())
                            }
                        );
                        builder.Exchange(exchange, ExchangeType.Direct);
                        // Queue and binding are in Delete mode for resources that do not exist on the broker.
                        builder.Queue(
                            queueName,
                            queue => queue.WithDeclareMode(RabbitMqDeclareMode.Delete)
                        );
                        builder.QueueBinding(
                            exchange,
                            queueName,
                            "idempotent.test",
                            binding => binding.WithBindingMode(RabbitMqBindingMode.Delete)
                        );
                    }
                );

            await using var serviceProvider = services.BuildServiceProvider();
            var hostedServices = serviceProvider.GetServices<IHostedService>().ToArray();

            var act = async () =>
            {
                foreach (var hostedService in hostedServices)
                {
                    await hostedService.StartAsync(cancellationToken);
                }
            };

            await act.Should().NotThrowAsync<Exception>();
        }
    }

    [Fact]
    public async Task DeleteBindingMode_SucceedsWhenBindingAlreadyAbsentAndQueueIsActive()
    {
        const string exchange = "policy-idempotent-binding-exchange";
        const string queueName = "policy-idempotent-binding-queue";
        var cancellationToken = TestContext.Current.CancellationToken;
        {
            // Provision a topology where the queue is Active but the binding is in Delete mode for a binding
            // that never existed on the broker. The unbind gets a 404 which is treated as success.
            var services = new ServiceCollection();
            services
               .AddTestCloudEvents()
               .AddRabbitMqTopology(
                    builder =>
                    {
                        builder.UseConnectionFactory(
                            _ => new ConnectionFactory
                            {
                                Uri = new Uri(_container.GetConnectionString())
                            }
                        );
                        builder.Exchange(exchange, ExchangeType.Direct);
                        builder.Queue(queueName);
                        builder.QueueBinding(
                            exchange,
                            queueName,
                            "idempotent.binding.test",
                            binding => binding.WithBindingMode(RabbitMqBindingMode.Delete)
                        );
                    }
                );

            await using var serviceProvider = services.BuildServiceProvider();
            var hostedServices = serviceProvider.GetServices<IHostedService>().ToArray();

            var act = async () =>
            {
                foreach (var hostedService in hostedServices)
                {
                    await hostedService.StartAsync(cancellationToken);
                }
            };

            await act.Should().NotThrowAsync<Exception>();
        }
    }

    private static async Task<BasicGetResult> GetRequiredMessageAsync(
        IChannel channel,
        string queueName,
        CancellationToken cancellationToken,
        int maximumAttempts = 40
    )
    {
        for (var attempt = 0; attempt < maximumAttempts; attempt++)
        {
            var result = await channel.BasicGetAsync(queueName, true, cancellationToken);

            if (result is not null)
            {
                return result;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(100), cancellationToken);
        }

        throw new InvalidOperationException($"No RabbitMQ message was available in queue '{queueName}'.");
    }

    private static async Task AssertQueueAbsentAsync(
        IChannel channel,
        string queueName,
        CancellationToken cancellationToken
    )
    {
        try
        {
            await channel.QueueDeclarePassiveAsync(queueName, cancellationToken);
            throw new InvalidOperationException($"Queue '{queueName}' should have been absent on the broker.");
        }
        catch (Exception ex) when (IsNotFound(ex))
        {
            // The queue is absent — expected.
        }
    }

    private static bool IsNotFound(Exception exception)
    {
        return exception is OperationInterruptedException { ShutdownReason: { ReplyCode: 404 } };
    }

    private static string GetDeathReason(BasicGetResult message)
    {
        return GetAmqpString(GetDeathValue(message, "reason"));
    }

    private static object? GetDeathValue(BasicGetResult message, string name)
    {
        if (message.BasicProperties.Headers is null ||
            !message.BasicProperties.Headers.TryGetValue("x-death", out var rawDeath) ||
            rawDeath is not IEnumerable<object?> deaths)
        {
            throw new InvalidOperationException("The RabbitMQ message does not contain an x-death header.");
        }

        foreach (var death in deaths)
        {
            if (death is IDictionary<string, object?> deathFields &&
                deathFields.TryGetValue(name, out var value))
            {
                return value;
            }
        }

        throw new InvalidOperationException($"The x-death header does not contain a '{name}' field.");
    }

    private static string GetAmqpString(object? value)
    {
        return value switch
        {
            byte[] bytes => Encoding.UTF8.GetString(bytes),
            string text => text,
            _ => value?.ToString() ?? string.Empty
        };
    }

    private sealed record PolicyDeliveryMessage(string Value) : ICloudEvent
    {
        Guid ICloudEvent.Id { get; } = BrilliantMessagingUuid.NewId();

        DateTimeOffset ICloudEvent.Time { get; } = DateTimeOffset.UtcNow;

        string? ICloudEvent.Subject => null;
    }

    private sealed class ThrowingPolicyDeliveryHandler : IMessageHandler<PolicyDeliveryMessage>
    {
        private readonly RedeliveryAttemptSink _sink;

        public ThrowingPolicyDeliveryHandler(RedeliveryAttemptSink sink)
        {
            _sink = sink;
        }

        public Task HandleAsync(
            PolicyDeliveryMessage message,
            IncomingMessageContext context,
            CancellationToken cancellationToken
        )
        {
            _sink.RecordAttempt();
            throw new InvalidOperationException("Handler always fails for delivery-limit test.");
        }
    }

    private sealed class RedeliveryAttemptSink
    {
        private readonly TaskCompletionSource<object?> _firstRedeliveryCompletion =
            new (TaskCreationOptions.RunContinuationsAsynchronously);

        private int _attemptCount;

        public IReadOnlyList<int> Attempts
        {
            get
            {
                var count = Volatile.Read(ref _attemptCount);
                var attempts = new List<int>();

                for (var i = 1; i <= count; i++)
                {
                    attempts.Add(i);
                }

                return attempts;
            }
        }

        public void RecordAttempt()
        {
            var count = Interlocked.Increment(ref _attemptCount);

            if (count == 2)
            {
                _firstRedeliveryCompletion.TrySetResult(null);
            }
        }

        public async Task WaitForRedeliveryAsync(CancellationToken cancellationToken)
        {
            await using var registration = cancellationToken.Register(
                static state => ((TaskCompletionSource<object?>) state!).TrySetCanceled(),
                _firstRedeliveryCompletion
            );
            await _firstRedeliveryCompletion.Task.ConfigureAwait(false);
        }
    }

    private sealed class RecordingPublishMessageHandler : IMessageHandler<RabbitMqPublishMessage>
    {
        private readonly ConsumedMessageSink _sink;

        public RecordingPublishMessageHandler(ConsumedMessageSink sink)
        {
            _sink = sink;
        }

        public Task HandleAsync(
            RabbitMqPublishMessage message,
            IncomingMessageContext context,
            CancellationToken cancellationToken
        )
        {
            _sink.Record(message);
            return Task.CompletedTask;
        }
    }

    private sealed class ConsumedMessageSink
    {
        private readonly TaskCompletionSource<RabbitMqPublishMessage> _completion =
            new (TaskCreationOptions.RunContinuationsAsynchronously);

        public void Record(RabbitMqPublishMessage message)
        {
            _completion.TrySetResult(message);
        }

        public async Task<RabbitMqPublishMessage> WaitAsync(CancellationToken cancellationToken)
        {
            await using var registration = cancellationToken.Register(
                static state => ((TaskCompletionSource<RabbitMqPublishMessage>) state!).TrySetCanceled(),
                _completion
            );
            return await _completion.Task.ConfigureAwait(false);
        }
    }
}
