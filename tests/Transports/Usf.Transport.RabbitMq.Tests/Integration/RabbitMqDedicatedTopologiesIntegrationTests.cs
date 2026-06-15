using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using Testcontainers.RabbitMq;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Serialization;
using Usf.Transport.RabbitMq.Tests.TestSupport;
using Xunit;

namespace Usf.Transport.RabbitMq.Tests.Integration;

[Collection<RabbitMqCollection>]
public sealed class RabbitMqDedicatedTopologiesIntegrationTests
{
    private readonly RabbitMqContainer _container;

    public RabbitMqDedicatedTopologiesIntegrationTests(RabbitMqFixture fixture)
    {
        _container = fixture.Container;
    }

    [Fact]
    public async Task QueueScopedConsumers_DispatchMixedTypesAndUseChannelCountForParallelism()
    {
        const string exchangeName = "queue-scoped-events";
        const string mixedQueue = "queue-scoped-mixed";
        const string parallelQueue = "queue-scoped-parallel";
        var cancellationToken = TestContext.Current.CancellationToken;
        {
            MixedMessageSink mixedSink = new ();
            ParallelHandlerProbe parallelProbe = new ();
            var services = new ServiceCollection();
            services.AddSingleton(mixedSink);
            services.AddSingleton(parallelProbe);
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
                        builder.Exchange(exchangeName, ExchangeType.Direct);
                        builder.Queue(mixedQueue);
                        builder.Queue(parallelQueue);
                        builder.QueueBinding(exchangeName, mixedQueue, "validation-a");
                        builder.QueueBinding(exchangeName, mixedQueue, "validation-b");
                        builder.QueueBinding(exchangeName, parallelQueue, "parallel");
                        builder.PublishNamed<ValidationMessageA>(
                            "validation-a",
                            target => target
                               .ToDirectExchange(exchangeName, "validation-a")
                               .WithSerializer<CloudEventMessageSerializer>()
                        );
                        builder.PublishNamed<ValidationMessageB>(
                            "validation-b",
                            target => target
                               .ToDirectExchange(exchangeName, "validation-b")
                               .WithSerializer<CloudEventMessageSerializer>()
                        );
                        builder.PublishNamed<ValidationMessageA>(
                            "parallel",
                            target => target
                               .ToDirectExchange(exchangeName, "parallel")
                               .WithSerializer<CloudEventMessageSerializer>()
                        );
                        builder.Consume(
                            mixedQueue,
                            consumer => consumer
                               .Handle<ValidationMessageA, MixedValidationMessageAHandler>()
                               .Handle<ValidationMessageB, MixedValidationMessageBHandler>()
                        );
                        builder.Consume(
                            parallelQueue,
                            consumer => consumer
                               .ChannelCount(2)
                               .PrefetchCount(1)
                               .Handle<ValidationMessageA, ParallelValidationMessageAHandler>()
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

                var topology = serviceProvider.GetRequiredService<RabbitMqTopology>();
                var publisher = serviceProvider.GetRequiredService<IMessagePublisher>();
                await publisher.PublishMessageAsync(
                    new ValidationMessageA("a"),
                    topology.GetRequiredTarget<ValidationMessageA>("validation-a"),
                    cancellationToken: cancellationToken
                );
                await publisher.PublishMessageAsync(
                    new ValidationMessageB("b"),
                    topology.GetRequiredTarget<ValidationMessageB>("validation-b"),
                    cancellationToken: cancellationToken
                );

                await mixedSink.WaitAsync(cancellationToken);

                await publisher.PublishMessageAsync(
                    new ValidationMessageA("first"),
                    topology.GetRequiredTarget<ValidationMessageA>("parallel"),
                    cancellationToken: cancellationToken
                );
                await publisher.PublishMessageAsync(
                    new ValidationMessageA("second"),
                    topology.GetRequiredTarget<ValidationMessageA>("parallel"),
                    cancellationToken: cancellationToken
                );

                await parallelProbe.WaitForBothAsync(cancellationToken);

                var connectionFactory = new ConnectionFactory
                {
                    Uri = new Uri(_container.GetConnectionString())
                };
                await using var connection = await connectionFactory.CreateConnectionAsync(cancellationToken);
                await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);

                mixedSink.Values.Should().BeEquivalentTo("a", "b");
                (await channel.ConsumerCountAsync(mixedQueue, cancellationToken)).Should().Be(1);
                (await channel.ConsumerCountAsync(parallelQueue, cancellationToken)).Should().Be(2);
            }
            finally
            {
                parallelProbe.Release();

                foreach (var hostedService in hostedServices.Reverse())
                {
                    await hostedService.StopAsync(CancellationToken.None);
                }
            }
        }
    }

    [Fact]
    public async Task NonCloudEventsPipeline_DeserializesRawBodyAndDeadLettersDeserializerFailures()
    {
        const string successQueue = "raw-success-queue";
        const string failureQueue = "raw-failure-queue";
        const string deadLetterQueue = "raw-dead-letter-queue";
        var cancellationToken = TestContext.Current.CancellationToken;
        {
            RawMessageSink sink = new ();
            RawInspector inspector = new ();
            RawMessageDeserializer deserializer = new ();
            var services = new ServiceCollection();
            services.AddSingleton(sink);
            services.AddSingleton(inspector);
            services.AddSingleton(deserializer);
            services.AddSingleton<ThrowingRawMessageDeserializer>();
            services
               .AddUsf()
               .UseCloudEvents(options => options.Source = "/tests/raw")
               .MapMessageContracts(
                    contracts =>
                    {
                        contracts.Map<RawMessage>(RawInspector.SuccessDiscriminator);
                        contracts.Map<RejectedRawMessage>(RawInspector.FailureDiscriminator);
                    }
                )
               .AddRabbitMqInboundTopology(
                    inbound => inbound
                       .UseConnectionFactory(
                            _ => new ConnectionFactory
                            {
                                Uri = new Uri(_container.GetConnectionString())
                            }
                        )
                       .Exchange("raw-exchange", ExchangeType.Direct)
                       .Exchange("raw-dead-letter-exchange", ExchangeType.Direct)
                       .Queue(successQueue)
                       .Queue(
                            failureQueue,
                            queue => queue
                               .WithDeadLetterExchange("raw-dead-letter-exchange")
                               .WithDeadLetterRoutingKey("failed")
                        )
                       .Queue(deadLetterQueue)
                       .QueueBinding("raw-exchange", successQueue, "success")
                       .QueueBinding("raw-exchange", failureQueue, "failure")
                       .QueueBinding("raw-dead-letter-exchange", deadLetterQueue, "failed")
                       .Consume(
                            successQueue,
                            endpoint => endpoint
                               .UseInspector<RawInspector>()
                               .Handle<RawMessage, RawMessageHandler>(
                                    handler => handler.WithDeserializer<RawMessageDeserializer>()
                                )
                        )
                       .Consume(
                            failureQueue,
                            endpoint => endpoint
                               .UseInspector<RawInspector>()
                               .Handle<RejectedRawMessage, RejectedRawMessageHandler>(
                                    handler => handler.WithDeserializer<ThrowingRawMessageDeserializer>()
                                )
                        )
                );

            await using var serviceProvider = services.BuildServiceProvider();
            var hostedServices = serviceProvider.GetServices<IHostedService>().ToArray();

            try
            {
                foreach (var hostedService in hostedServices)
                {
                    await hostedService.StartAsync(cancellationToken);
                }

                var connectionFactory = new ConnectionFactory
                {
                    Uri = new Uri(_container.GetConnectionString())
                };
                await using var connection = await connectionFactory.CreateConnectionAsync(cancellationToken);
                await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);

                await channel.BasicPublishAsync(
                    "raw-exchange",
                    "success",
                    mandatory: true,
                    new BasicProperties
                    {
                        ContentType = "text/plain"
                    },
                    "42|raw"u8.ToArray(),
                    cancellationToken
                );
                await channel.BasicPublishAsync(
                    "raw-exchange",
                    "failure",
                    mandatory: true,
                    new BasicProperties
                    {
                        ContentType = "application/octet-stream"
                    },
                    "invalid"u8.ToArray(),
                    cancellationToken
                );

                var consumed = await sink.WaitAsync(cancellationToken);
                var deadLetter = await GetRequiredMessageAsync(channel, deadLetterQueue, cancellationToken);

                consumed.Message.Should().Be(new RawMessage(42, "raw"));
                consumed.Context.MessageType.Should().Be(typeof(RawMessage));
                consumed.Context.Message.Should().BeSameAs(consumed.Message);
                inspector.InspectionCount.Should().Be(2);
                deserializer.CallCount.Should().Be(1);
                deserializer.MessageWasNull.Should().BeTrue();
                Encoding.UTF8.GetString(deadLetter.Body.ToArray()).Should().Be("invalid");
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
    public async Task DedicatedOutboundAndInboundTopologies_PublishAndConsumeAcrossTwoConnections()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        {
            ConsumedMessageSink sink = new ();
            var services = new ServiceCollection();
            services.AddSingleton(sink);
            services
               .AddTestCloudEvents()
               .AddRabbitMqOutboundTopology(
                    outbound => outbound
                       .UseConnectionFactory(
                            _ => new ConnectionFactory
                            {
                                Uri = new Uri(_container.GetConnectionString())
                            }
                        )
                       .Exchange("inbound-events", ExchangeType.Direct)
                       .Publish<RabbitMqPublishMessage>(
                            target => target
                               .ToDirectExchange("inbound-events", "published")
                               .WithSerializer<CloudEventMessageSerializer>()
                        )
                )
               .AddRabbitMqInboundTopology(
                    inbound => inbound
                       .UseConnectionFactory(
                            _ => new ConnectionFactory
                            {
                                Uri = new Uri(_container.GetConnectionString())
                            }
                        )
                       .Exchange("inbound-events", ExchangeType.Direct)
                       .Queue("inbound-events-queue")
                       .QueueBinding("inbound-events", "inbound-events-queue", "published")
                       .Consume(
                            "inbound-events-queue",
                            endpoint => endpoint
                               .PrefetchCount(1)
                               .Concurrency(1)
                               .Handle<RabbitMqPublishMessage, RecordingPublishMessageHandler>()
                        )
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
                    new RabbitMqPublishMessage(42, "consumed"),
                    cancellationToken: cancellationToken
                );

                var consumed = await sink.WaitAsync(cancellationToken);

                consumed.Id.Should().Be(42);
                consumed.Name.Should().Be("consumed");

                var outboundTopology =
                    serviceProvider.GetRequiredKeyedService<RabbitMqTopology>(Topology.DefaultName);
                var inboundTopology =
                    serviceProvider.GetRequiredKeyedService<RabbitMqTopology>(RabbitMqTopology.DefaultInboundName);
                var outboundConnection = await outboundTopology.GetConnectionAsync(cancellationToken);
                var inboundConnection = await inboundTopology.GetConnectionAsync(cancellationToken);

                outboundConnection.Should().NotBeSameAs(inboundConnection);
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

    private static async Task<BasicGetResult> GetRequiredMessageAsync(
        IChannel channel,
        string queueName,
        CancellationToken cancellationToken
    )
    {
        for (var attempt = 0; attempt < 40; attempt++)
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

    private sealed class MixedMessageSink
    {
        private readonly TaskCompletionSource<bool> _completed =
            new (TaskCreationOptions.RunContinuationsAsynchronously);

        private readonly object _sync = new ();

        public IList<string> Values { get; } = new List<string>();

        public void Add(string value)
        {
            lock (_sync)
            {
                Values.Add(value);

                if (Values.Count == 2)
                {
                    _completed.TrySetResult(true);
                }
            }
        }

        public Task WaitAsync(CancellationToken cancellationToken)
        {
            return _completed.Task.WaitAsync(cancellationToken);
        }
    }

    private sealed class ParallelHandlerProbe
    {
        private readonly TaskCompletionSource<bool> _bothEntered =
            new (TaskCreationOptions.RunContinuationsAsynchronously);

        private readonly TaskCompletionSource<bool> _release =
            new (TaskCreationOptions.RunContinuationsAsynchronously);

        private int _entered;

        public async Task EnterAsync(CancellationToken cancellationToken)
        {
            if (Interlocked.Increment(ref _entered) == 2)
            {
                _bothEntered.TrySetResult(true);
            }

            await _release.Task.WaitAsync(cancellationToken);
        }

        public Task WaitForBothAsync(CancellationToken cancellationToken)
        {
            return _bothEntered.Task.WaitAsync(cancellationToken);
        }

        public void Release()
        {
            _release.TrySetResult(true);
        }
    }

    private sealed class MixedValidationMessageAHandler : IMessageHandler<ValidationMessageA>
    {
        private readonly MixedMessageSink _sink;

        public MixedValidationMessageAHandler(MixedMessageSink sink)
        {
            _sink = sink;
        }

        public Task HandleAsync(
            ValidationMessageA message,
            IncomingMessageContext context,
            CancellationToken cancellationToken
        )
        {
            _sink.Add(message.Value);
            return Task.CompletedTask;
        }
    }

    private sealed class MixedValidationMessageBHandler : IMessageHandler<ValidationMessageB>
    {
        private readonly MixedMessageSink _sink;

        public MixedValidationMessageBHandler(MixedMessageSink sink)
        {
            _sink = sink;
        }

        public Task HandleAsync(
            ValidationMessageB message,
            IncomingMessageContext context,
            CancellationToken cancellationToken
        )
        {
            _sink.Add(message.Value);
            return Task.CompletedTask;
        }
    }

    private sealed class ParallelValidationMessageAHandler : IMessageHandler<ValidationMessageA>
    {
        private readonly ParallelHandlerProbe _probe;

        public ParallelValidationMessageAHandler(ParallelHandlerProbe probe)
        {
            _probe = probe;
        }

        public Task HandleAsync(
            ValidationMessageA message,
            IncomingMessageContext context,
            CancellationToken cancellationToken
        )
        {
            return _probe.EnterAsync(cancellationToken);
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

    // ReSharper disable NotAccessedPositionalProperty.Local -- required for serialization
    private sealed record RawMessage(int Id, string Name);
    // ReSharper restore NotAccessedPositionalProperty.Local

    private sealed record RejectedRawMessage;

    private sealed class RawInspector : IInboundMessageInspector
    {
        public const string FailureDiscriminator = "tests.raw.failure";
        public const string SuccessDiscriminator = "tests.raw.success";
        private int _inspectionCount;

        public int InspectionCount => Volatile.Read(ref _inspectionCount);

        public ValueTask<InboundMessageInspectionResult> InspectAsync(
            TransportMessage transportMessage,
            CancellationToken cancellationToken = default
        )
        {
            Interlocked.Increment(ref _inspectionCount);
            return transportMessage.Source switch
            {
                "raw-success-queue" => new ValueTask<InboundMessageInspectionResult>(
                    new InboundMessageInspectionResult(SuccessDiscriminator, typeof(RawMessage))
                ),
                "raw-failure-queue" => new ValueTask<InboundMessageInspectionResult>(
                    new InboundMessageInspectionResult(FailureDiscriminator, typeof(RejectedRawMessage))
                ),
                _ => throw new InvalidOperationException($"Unexpected raw source '{transportMessage.Source}'.")
            };
        }
    }

    private sealed class RawMessageDeserializer : IMessageDeserializer
    {
        public int CallCount { get; private set; }

        public bool MessageWasNull { get; private set; }

        public ValueTask<object?> DeserializeAsync(
            IncomingMessageContext context,
            CancellationToken cancellationToken = default
        )
        {
            CallCount++;
            MessageWasNull = context.Message is null;
            context.MessageType.Should().Be(typeof(RawMessage));
            var parts = Encoding.UTF8.GetString(context.Transport.Body.Span).Split('|');
            return new ValueTask<object?>(new RawMessage(int.Parse(parts[0]), parts[1]));
        }
    }

    private sealed class ThrowingRawMessageDeserializer : IMessageDeserializer
    {
        public ValueTask<object?> DeserializeAsync(
            IncomingMessageContext context,
            CancellationToken cancellationToken = default
        )
        {
            throw new InvalidOperationException("Raw payload rejected.");
        }
    }

    private sealed class RawMessageHandler : IMessageHandler<RawMessage>
    {
        private readonly RawMessageSink _sink;

        public RawMessageHandler(RawMessageSink sink)
        {
            _sink = sink;
        }

        public Task HandleAsync(
            RawMessage message,
            IncomingMessageContext context,
            CancellationToken cancellationToken
        )
        {
            _sink.Record(message, context);
            return Task.CompletedTask;
        }
    }

    private sealed class RejectedRawMessageHandler : IMessageHandler<RejectedRawMessage>
    {
        public Task HandleAsync(
            RejectedRawMessage message,
            IncomingMessageContext context,
            CancellationToken cancellationToken
        )
        {
            throw new InvalidOperationException("The rejected raw message must not reach its handler.");
        }
    }

    private sealed class RawMessageSink
    {
        private readonly TaskCompletionSource<(RawMessage Message, IncomingMessageContext Context)> _completion =
            new (TaskCreationOptions.RunContinuationsAsynchronously);

        public void Record(RawMessage message, IncomingMessageContext context)
        {
            _completion.TrySetResult((message, context));
        }

        public async Task<(RawMessage Message, IncomingMessageContext Context)> WaitAsync(
            CancellationToken cancellationToken
        )
        {
            using var registration = cancellationToken.Register(
                static state =>
                    ((TaskCompletionSource<(RawMessage, IncomingMessageContext)>) state!).TrySetCanceled(),
                _completion
            );
            return await _completion.Task.ConfigureAwait(false);
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
            using var registration = cancellationToken.Register(
                static state => ((TaskCompletionSource<RabbitMqPublishMessage>) state!).TrySetCanceled(),
                _completion
            );
            return await _completion.Task.ConfigureAwait(false);
        }
    }
}
