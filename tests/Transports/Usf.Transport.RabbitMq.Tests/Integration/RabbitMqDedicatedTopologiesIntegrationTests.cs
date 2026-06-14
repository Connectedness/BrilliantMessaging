using System;
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

public sealed class RabbitMqDedicatedTopologiesIntegrationTests
{
    [Fact]
    public async Task NonCloudEventsPipeline_DeserializesRawBodyAndDeadLettersDeserializerFailures()
    {
        const string successQueue = "raw-success-queue";
        const string failureQueue = "raw-failure-queue";
        const string deadLetterQueue = "raw-dead-letter-queue";
        var cancellationToken = TestContext.Current.CancellationToken;
        var container = new RabbitMqBuilder("public.ecr.aws/docker/library/rabbitmq:3.13.7-management").Build();
        await container.StartAsync(cancellationToken);

        try
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
                                Uri = new Uri(container.GetConnectionString())
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
                               .WithDeserializer<RawMessageDeserializer>()
                               .Handle<RawMessage, RawMessageHandler>()
                        )
                       .Consume(
                            failureQueue,
                            endpoint => endpoint
                               .UseInspector<RawInspector>()
                               .WithDeserializer<ThrowingRawMessageDeserializer>()
                               .Handle<RejectedRawMessage, RejectedRawMessageHandler>()
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
                    Uri = new Uri(container.GetConnectionString())
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
        finally
        {
            await container.DisposeAsync();
        }
    }

    [Fact]
    public async Task DedicatedOutboundAndInboundTopologies_PublishAndConsumeAcrossTwoConnections()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var container = new RabbitMqBuilder("public.ecr.aws/docker/library/rabbitmq:3.13.7-management").Build();
        await container.StartAsync(cancellationToken);

        try
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
                                Uri = new Uri(container.GetConnectionString())
                            }
                        )
                       .Exchange("inbound-events", ExchangeType.Direct)
                       .Address("inbound-events-address", "inbound-events")
                       .Publish<RabbitMqPublishMessage>(
                            target => target
                               .ToDirectAddress("inbound-events-address", "published")
                               .WithSerializer<CloudEventMessageSerializer>()
                        )
                )
               .AddRabbitMqInboundTopology(
                    inbound => inbound
                       .UseConnectionFactory(
                            _ => new ConnectionFactory
                            {
                                Uri = new Uri(container.GetConnectionString())
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
        finally
        {
            await container.DisposeAsync();
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

    private sealed record RawMessage(int Id, string Name);

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
            Type messageType,
            CancellationToken cancellationToken = default
        )
        {
            CallCount++;
            MessageWasNull = context.Message is null;
            messageType.Should().Be(typeof(RawMessage));
            var parts = Encoding.UTF8.GetString(context.Transport.Body.Span).Split('|');
            return new ValueTask<object?>(new RawMessage(int.Parse(parts[0]), parts[1]));
        }
    }

    private sealed class ThrowingRawMessageDeserializer : IMessageDeserializer
    {
        public ValueTask<object?> DeserializeAsync(
            IncomingMessageContext context,
            Type messageType,
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
