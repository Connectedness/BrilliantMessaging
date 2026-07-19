using System;
using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;
using BrilliantMessaging.Transport.Nats.Inbound;
using BrilliantMessaging.Transport.Nats.Tests.TestSupport;
using FluentAssertions;
using NATS.Client.Core;
using Xunit;

namespace BrilliantMessaging.Transport.Nats.Tests.Unit;

public sealed class NatsConfigurationBuilderTests
{
    [Fact]
    public void StreamBuilder_CapturesAllJetStreamPolicyKnobs()
    {
        NatsStreamBuilder builder = new ("ORDERS");

        builder.Subject("orders.*")
           .Subject("orders.dead")
           .DuplicateWindow(TimeSpan.FromMinutes(2))
           .MaxAge(TimeSpan.FromHours(1))
           .MaxMessageSize(4096)
           .Storage(NatsStreamStorage.Memory)
           .Retention(NatsStreamRetention.WorkQueue)
           .Replicas(3);

        var definition = ((IBuildable<NatsStreamDefinition>) builder).Build();

        definition.Name.Should().Be("ORDERS");
        definition.Subjects.Should().Equal("orders.*", "orders.dead");
        definition.DuplicateWindow.Should().Be(TimeSpan.FromMinutes(2));
        definition.MaxAge.Should().Be(TimeSpan.FromHours(1));
        definition.MaxMessageSize.Should().Be(4096);
        definition.Storage.Should().Be(NatsStreamStorage.Memory);
        definition.Retention.Should().Be(NatsStreamRetention.WorkQueue);
        definition.Replicas.Should().Be(3);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void StreamBuilder_RejectsBlankName(string name)
    {
        var act = () => new NatsStreamBuilder(name);

        act.Should().Throw<ArgumentException>().WithParameterName("name");
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void StreamBuilder_RejectsBlankSubject(string subject)
    {
        NatsStreamBuilder builder = new ("ORDERS");

        var act = () => builder.Subject(subject);

        act.Should().Throw<ArgumentException>().WithParameterName("subjectPattern");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void StreamBuilder_RejectsInvalidMaxMessageSize(int bytes)
    {
        NatsStreamBuilder builder = new ("ORDERS");

        var act = () => builder.MaxMessageSize(bytes);

        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("bytes");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void StreamBuilder_RejectsInvalidReplicaCount(int replicas)
    {
        NatsStreamBuilder builder = new ("ORDERS");

        var act = () => builder.Replicas(replicas);

        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("replicas");
    }

    [Fact]
    public void StreamBuilder_RejectsInvalidTimeSpans()
    {
        NatsStreamBuilder builder = new ("ORDERS");

        var duplicateWindow = () => builder.DuplicateWindow(TimeSpan.Zero);
        var maxAge = () => builder.MaxAge(TimeSpan.Zero);

        duplicateWindow.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("duplicateWindow");
        maxAge.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("maxAge");
    }

    [Fact]
    public void StreamBuilder_RejectsUndefinedEnums()
    {
        NatsStreamBuilder builder = new ("ORDERS");

        var storage = () => builder.Storage((NatsStreamStorage) 99);
        var retention = () => builder.Retention((NatsStreamRetention) 99);

        storage.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("storage");
        retention.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("retention");
    }

    [Fact]
    public void TopologyBuilder_CapturesOptionsFactoryLocalContractsAndRuntimeOptions()
    {
        NatsTopologyBuilder builder = new ();
        NatsOpts options = new () { Url = "nats://example:4222" };

        builder.UseOptions(_ => options)
           .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("local.order.placed"))
           .Stream("ORDERS", stream => stream.Subject("orders.*"))
           .Provisioning(NatsTopologyProvisioningMode.AssertOnly)
           .Consume(
                "ORDERS",
                "orders-worker",
                consumer => consumer.Handle<OrderPlaced, OrderPlacedHandler>()
            )
           .UseDeserializationMiddleware<PassThroughMiddleware>()
           .WithShutdownTimeout(TimeSpan.FromSeconds(7))
           .AckProgress(false);

        var configuration = ((IBuildable<NatsTopologyConfiguration>) builder).Build();

        configuration.CreateOptions.Should().NotBeNull();
        configuration.CreateOptions!(EmptyServiceProvider.Instance).Should().BeSameAs(options);
        configuration.MessageContractDialect.Should().NotBeNull();
        configuration.MessageContractDialect!.GetDiscriminator(typeof(OrderPlaced)).Should().Be("local.order.placed");
        configuration.Streams.Should().ContainSingle().Which.Name.Should().Be("ORDERS");
        configuration.Consumers.Should().ContainSingle().Which.DurableName.Should().Be("orders-worker");
        configuration.DeserializationMiddlewareType.Should().Be<PassThroughMiddleware>();
        configuration.ShutdownTimeout.Should().Be(TimeSpan.FromSeconds(7));
        configuration.ProvisioningMode.Should().Be(NatsTopologyProvisioningMode.AssertOnly);
        configuration.AckProgressEnabled.Should().BeFalse();
    }

    [Fact]
    public void TopologyBuilder_CapturesConcreteOptions()
    {
        NatsTopologyBuilder builder = new ();
        NatsOpts options = new () { Url = "nats://example:4222" };

        builder.UseOptions(options);

        var configuration = ((IBuildable<NatsTopologyConfiguration>) builder).Build();

        configuration.CreateOptions!(EmptyServiceProvider.Instance).Should().BeSameAs(options);
    }

    [Fact]
    public void TopologyBuilder_AllowsInfiniteShutdownTimeout()
    {
        NatsTopologyBuilder builder = new ();

        builder.WithShutdownTimeout(Timeout.InfiniteTimeSpan);

        var configuration = ((IBuildable<NatsTopologyConfiguration>) builder).Build();

        configuration.ShutdownTimeout.Should().Be(Timeout.InfiniteTimeSpan);
    }

    [Fact]
    public void TopologyBuilder_RejectsInvalidArguments()
    {
        NatsTopologyBuilder builder = new ();

        Action useOptions = () => builder.UseOptions((NatsOpts) null!);
        Action useOptionsFactory = () => builder.UseOptions((Func<IServiceProvider, NatsOpts>) null!);
        Action mapContracts = () => builder.MapMessageContracts(null!);
        Action stream = () => builder.Stream("ORDERS", null!);
        Action provisioning = () => builder.Provisioning((NatsTopologyProvisioningMode) 99);
        Action publish = () => builder.Publish<OrderPlaced>(null!);
        Action publishNamed = () => builder.PublishNamed<OrderPlaced>("audit", null!);
        Action consume = () => builder.Consume("ORDERS", "orders-worker", null!);
        Action pipeline = () => builder.ConfigureInboundPipeline(null!);
        Action shutdown = () => builder.WithShutdownTimeout(TimeSpan.Zero);

        useOptions.Should().Throw<ArgumentNullException>().WithParameterName("options");
        useOptionsFactory.Should().Throw<ArgumentNullException>().WithParameterName("createOptions");
        mapContracts.Should().Throw<ArgumentNullException>().WithParameterName("configure");
        stream.Should().Throw<ArgumentNullException>().WithParameterName("configure");
        provisioning.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("mode");
        publish.Should().Throw<ArgumentNullException>().WithParameterName("configure");
        publishNamed.Should().Throw<ArgumentNullException>().WithParameterName("configure");
        consume.Should().Throw<ArgumentNullException>().WithParameterName("configure");
        pipeline.Should().Throw<ArgumentNullException>().WithParameterName("configure");
        shutdown.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("shutdownTimeout");
    }

    [Fact]
    public void TopologyBuilder_RejectsBlankNamedTarget()
    {
        NatsTopologyBuilder builder = new ();

        var act = () => builder.PublishNamed<OrderPlaced>(" ", target => target.ToSubject("orders.audit"));

        act.Should().Throw<ArgumentException>().WithParameterName("targetName");
    }

    [Fact]
    public void InboundConsumerBuilder_CapturesConsumerAndHandlerRedeliveryClassifiers()
    {
        NatsInboundConsumerBuilder builder = new ("ORDERS", "orders-worker");

        builder.WithRedelivery(redelivery => redelivery.ShouldRetry(static failure => failure is TimeoutException))
           .Handle<OrderPlaced, OrderPlacedHandler>()
           .HandleNamed<OrderCancelled, OrderCancelledHandler>(
                "cancelled",
                handler => handler.WithRedelivery(
                    redelivery => redelivery.ShouldRetry(static failure => failure is ApplicationException)
                )
            );

        var definition = ((IBuildable<NatsInboundConsumerDefinition>) builder).Build();

        definition.RedeliveryClassifier.Should().NotBeNull();
        definition.RedeliveryClassifier!.ShouldRetry(new TimeoutException()).Should().BeTrue();
        definition.RedeliveryClassifier.ShouldRetry(new ApplicationException()).Should().BeFalse();

        var defaultHandler = definition.Handlers[0];
        defaultHandler.RedeliveryClassifier.Should().BeNull();

        var overrideHandler = definition.Handlers[1];
        overrideHandler.EndpointName.Should().Be("cancelled");
        overrideHandler.RedeliveryClassifier.Should().NotBeNull();
        overrideHandler.RedeliveryClassifier!.ShouldRetry(new TimeoutException()).Should().BeFalse();
        overrideHandler.RedeliveryClassifier.ShouldRetry(new ApplicationException()).Should().BeTrue();
    }

    [Fact]
    public void InboundHandlerBuilder_CanRestoreAutoAckAndOverrideDeserializer()
    {
        NatsInboundHandlerBuilder builder = new ();

        builder.ManualAck().AutoAck().WithDeserializer<RecordingDeserializer>();

        var configuration = ((IBuildable<NatsInboundHandlerConfiguration>) builder).Build();

        configuration.AckMode.Should().Be(MessageAckMode.Auto);
        configuration.DeserializerType.Should().Be<RecordingDeserializer>();
    }

    [Fact]
    public void InboundConsumerBuilder_EnforcesMinimumAckWait()
    {
        NatsInboundConsumerBuilder builder = new ("ORDERS", "orders-worker");

        var belowMinimum = () => builder.AckWait(TimeSpan.FromSeconds(3) - TimeSpan.FromMilliseconds(1));
        var atMinimum = () => builder.AckWait(TimeSpan.FromSeconds(3));

        belowMinimum.Should()
           .Throw<ArgumentOutOfRangeException>()
           .WithParameterName("ackWait")
           .WithMessage("*at least 3 seconds*AckProgress*");
        atMinimum.Should().NotThrow();
    }

    [Fact]
    public void InboundBuilders_RejectInvalidArguments()
    {
        var blankStream = () => new NatsInboundConsumerBuilder("", "orders-worker");
        var blankDurable = () => new NatsInboundConsumerBuilder("ORDERS", "");
        NatsInboundConsumerBuilder consumerBuilder = new ("ORDERS", "orders-worker");

        Action filter = () => consumerBuilder.FilterSubject(" ");
        Action ackWait = () => consumerBuilder.AckWait(TimeSpan.Zero);
        Action maxDeliver = () => consumerBuilder.MaxDeliver(0);
        Action maxAckPending = () => consumerBuilder.MaxAckPending(0);
        Action maxBufferedMessages = () => consumerBuilder.MaxBufferedMessages(0);
        Action deadLetter = () => consumerBuilder.DeadLetterSubject(" ");
        Action consumerRedelivery = () => consumerBuilder.WithRedelivery(null!);
        Action abstractHandler = () => consumerBuilder.Handle<OrderPlaced, AbstractOrderPlacedHandler>();
        NatsInboundHandlerBuilder handlerBuilder = new ();
        Action handlerRedelivery = () => handlerBuilder.WithRedelivery(null!);

        blankStream.Should().Throw<ArgumentException>().WithParameterName("streamName");
        blankDurable.Should().Throw<ArgumentException>().WithParameterName("durableName");
        filter.Should().Throw<ArgumentException>().WithParameterName("subject");
        ackWait.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("ackWait");
        maxDeliver.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("maxDeliver");
        maxAckPending.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("maxAckPending");
        maxBufferedMessages.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("maxBufferedMessages");
        deadLetter.Should().Throw<ArgumentException>().WithParameterName("subject");
        consumerRedelivery.Should().Throw<ArgumentNullException>().WithParameterName("configure");
        abstractHandler.Should().Throw<ArgumentException>().WithParameterName("THandler");
        handlerRedelivery.Should().Throw<ArgumentNullException>().WithParameterName("configure");
    }

    private sealed class EmptyServiceProvider : IServiceProvider
    {
        public static EmptyServiceProvider Instance { get; } = new ();

        public object? GetService(Type serviceType)
        {
            return null;
        }
    }

    private sealed class PassThroughMiddleware : IMessageMiddleware
    {
        public Task InvokeAsync(IncomingMessageContext context, MessageDelegate next)
        {
            return next(context);
        }
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

    private abstract class AbstractOrderPlacedHandler : IMessageHandler<OrderPlaced>
    {
        public abstract Task HandleAsync(
            OrderPlaced message,
            IncomingMessageContext context,
            CancellationToken cancellationToken = default
        );
    }
}
