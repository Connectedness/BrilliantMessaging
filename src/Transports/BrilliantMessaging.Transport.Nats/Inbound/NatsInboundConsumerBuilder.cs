using System;
using System.Collections.Immutable;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;

namespace BrilliantMessaging.Transport.Nats.Inbound;

/// <summary>
/// Fluent builder for a durable JetStream consumer.
/// </summary>
public sealed class NatsInboundConsumerBuilder : IBuildable<NatsInboundConsumerDefinition>
{
    private readonly string _durableName;

    private readonly ImmutableArray<NatsInboundHandlerDefinition>.Builder _handlers =
        ImmutableArray.CreateBuilder<NatsInboundHandlerDefinition>();

    private readonly string _streamName;
    private TimeSpan _ackWait = NatsTopologyBuilderDefaults.DefaultAckWait;
    private int _concurrency = 1;
    private int _deadLetterAfterDeliveryAttempt = 5;
    private string? _deadLetterSubject;
    private string? _filterSubject;
    private int _maxAckPending = 1024;
    private int _maxBufferedMessages = NatsTopologyBuilderDefaults.DefaultMaxBufferedMessages;
    private int _maxDeliver = 10;
    private RedeliveryClassifier? _redeliveryClassifier;

    /// <summary>
    /// Initializes a new instance of the <see cref="NatsInboundConsumerBuilder" /> class.
    /// </summary>
    public NatsInboundConsumerBuilder(string streamName, string durableName)
    {
        _streamName = RequireText(streamName, nameof(streamName));
        _durableName = RequireText(durableName, nameof(durableName));
    }

    /// <inheritdoc />
    NatsInboundConsumerDefinition IBuildable<NatsInboundConsumerDefinition>.Build()
    {
        if (_deadLetterAfterDeliveryAttempt > _maxDeliver)
        {
            throw new InvalidOperationException(
                $"DeadLetterAfterDeliveryAttempt ({_deadLetterAfterDeliveryAttempt}) must not exceed MaxDeliver ({_maxDeliver})."
            );
        }

        return new NatsInboundConsumerDefinition(
            _streamName,
            _durableName,
            _filterSubject,
            _concurrency,
            _ackWait,
            _maxDeliver,
            _deadLetterAfterDeliveryAttempt,
            _maxAckPending,
            _maxBufferedMessages,
            _deadLetterSubject,
            _redeliveryClassifier,
            _handlers.ToImmutable()
        );
    }

    /// <summary>
    /// Restricts the durable consumer to a NATS subject pattern. The pattern may include
    /// <c>*</c> and terminal <c>&gt;</c> wildcards and must overlap the referenced stream.
    /// </summary>
    public NatsInboundConsumerBuilder FilterSubject(string subject)
    {
        _filterSubject = RequireText(subject, nameof(subject));
        return this;
    }

    /// <summary>
    /// Sets the number of concurrent message-processing loops for this durable consumer.
    /// </summary>
    public NatsInboundConsumerBuilder Concurrency(int concurrency)
    {
        if (concurrency <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(concurrency), concurrency, "The value must be positive.");
        }

        _concurrency = concurrency;
        return this;
    }

    /// <summary>
    /// Sets JetStream AckWait for this durable consumer. Values below
    /// <see cref="NatsTopologyBuilderDefaults.MinimumAckWait" /> are rejected because the AckProgress
    /// heartbeat could no longer keep an in-flight delivery alive.
    /// </summary>
    public NatsInboundConsumerBuilder AckWait(TimeSpan ackWait)
    {
        if (ackWait < NatsTopologyBuilderDefaults.MinimumAckWait)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ackWait),
                ackWait,
                $"The value must be at least {NatsTopologyBuilderDefaults.MinimumAckWait.TotalSeconds} seconds so the AckProgress heartbeat (AckWait / 3) fires safely before the ack deadline."
            );
        }

        _ackWait = ackWait;
        return this;
    }

    /// <summary>
    /// Sets JetStream MaxDeliver for this durable consumer.
    /// </summary>
    public NatsInboundConsumerBuilder MaxDeliver(int maxDeliver)
    {
        if (maxDeliver <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxDeliver), maxDeliver, "The value must be positive.");
        }

        _maxDeliver = maxDeliver;
        return this;
    }

    /// <summary>
    /// Sets the JetStream delivery attempt on which a normally failed delivery is dead-lettered or
    /// terminated. JetStream counts every delivery in <c>NumDelivered</c>, including deliveries caused
    /// by shutdown interruption or acknowledgement timeout, so this is a delivery ordinal rather than
    /// a durable handler-failure counter. The value must not exceed <see cref="MaxDeliver" />.
    /// </summary>
    public NatsInboundConsumerBuilder DeadLetterAfterDeliveryAttempt(int deliveryAttempt)
    {
        if (deliveryAttempt <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(deliveryAttempt),
                deliveryAttempt,
                "The value must be positive."
            );
        }

        _deadLetterAfterDeliveryAttempt = deliveryAttempt;
        return this;
    }

    /// <summary>
    /// Sets JetStream MaxAckPending for this durable consumer.
    /// </summary>
    public NatsInboundConsumerBuilder MaxAckPending(int maxAckPending)
    {
        if (maxAckPending <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxAckPending),
                maxAckPending,
                "The value must be positive."
            );
        }

        _maxAckPending = maxAckPending;
        return this;
    }

    /// <summary>
    /// Sets how many messages each worker buffers client-side per pull request. Buffered messages are not
    /// heartbeated while they wait for the sequential dispatch loop, so keep
    /// <c>maxBufferedMessages × worst-case handler duration</c> well below AckWait to avoid redeliveries.
    /// </summary>
    public NatsInboundConsumerBuilder MaxBufferedMessages(int maxBufferedMessages)
    {
        if (maxBufferedMessages <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxBufferedMessages),
                maxBufferedMessages,
                "The value must be positive."
            );
        }

        _maxBufferedMessages = maxBufferedMessages;
        return this;
    }

    /// <summary>
    /// Configures a subject used for rejected or exhausted deliveries.
    /// </summary>
    public NatsInboundConsumerBuilder DeadLetterSubject(string subject)
    {
        _deadLetterSubject = RequireText(subject, nameof(subject));
        return this;
    }

    /// <summary>
    /// Configures a consumer-wide redelivery classifier.
    /// </summary>
    public NatsInboundConsumerBuilder WithRedelivery(Action<RedeliveryClassifierBuilder> configure)
    {
        if (configure is null)
        {
            throw new ArgumentNullException(nameof(configure));
        }

        RedeliveryClassifierBuilder builder = new ();
        configure(builder);
        _redeliveryClassifier = ((IBuildable<RedeliveryClassifier>) builder).Build();
        return this;
    }

    /// <summary>
    /// Adds a handler for <typeparamref name="TMessage" />.
    /// </summary>
    public NatsInboundConsumerBuilder Handle<TMessage, THandler>(
        Action<NatsInboundHandlerBuilder>? configure = null
    )
        where THandler : class, IMessageHandler<TMessage>
    {
        return HandleNamed<TMessage, THandler>(endpointName: null, configure);
    }

    /// <summary>
    /// Adds a named handler for <typeparamref name="TMessage" />.
    /// </summary>
    public NatsInboundConsumerBuilder HandleNamed<TMessage, THandler>(
        string? endpointName,
        Action<NatsInboundHandlerBuilder>? configure = null
    )
        where THandler : class, IMessageHandler<TMessage>
    {
        if (typeof(THandler).IsInterface || typeof(THandler).IsAbstract)
        {
            throw new ArgumentException(
                $"Handler type '{typeof(THandler)}' must be a concrete class.",
                nameof(THandler)
            );
        }

        NatsInboundHandlerBuilder builder = new ();
        configure?.Invoke(builder);
        var configuration = ((IBuildable<NatsInboundHandlerConfiguration>) builder).Build();

        _handlers.Add(
            new NatsInboundHandlerDefinition(
                endpointName,
                typeof(TMessage),
                typeof(THandler),
                MessageHandlerInvocation.Create<TMessage, THandler>(),
                configuration.DeserializerType,
                configuration.AckMode,
                configuration.RedeliveryClassifier
            )
        );
        return this;
    }

    private static string RequireText(string value, string parameterName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", parameterName);
        }

        return value;
    }
}
