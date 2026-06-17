using System;
using System.Collections.Generic;
using Bmf.Core.Messaging;
using Bmf.Core.Messaging.Inbound;

namespace Bmf.Transport.RabbitMq.Inbound;

/// <summary>
/// Fluent builder for a RabbitMQ consumer on a single queue. It configures the prefetch, concurrency, channel
/// count, channel group, message inspector, and body-copy behaviour, and registers one or more typed handlers.
/// </summary>
public sealed class RabbitMqInboundConsumerBuilder
{
    private readonly List<RabbitMqInboundHandlerDefinition> _handlers = [];
    private readonly string _queueName;
    private int _channelCount = 1;
    private string? _channelGroupName;
    private ushort _consumerDispatchConcurrency = 1;
    private bool _copyBody = true;
    private Type _inspectorType = typeof(CloudEventsInboundMessageInspector);
    private ushort _prefetchCount = 1;

    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMqInboundConsumerBuilder" /> class for the given queue.
    /// </summary>
    /// <param name="queueName">The name of the queue to consume.</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="queueName" /> is null or whitespace.</exception>
    public RabbitMqInboundConsumerBuilder(string queueName)
    {
        _queueName = RequireText(queueName, nameof(queueName));
    }

    /// <summary>
    /// Sets the per-consumer prefetch (QoS) count.
    /// </summary>
    /// <param name="prefetchCount">The prefetch count; must be greater than zero.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="prefetchCount" /> is zero.</exception>
    public RabbitMqInboundConsumerBuilder PrefetchCount(ushort prefetchCount)
    {
        if (prefetchCount == 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(prefetchCount),
                prefetchCount,
                "The value must be greater than zero."
            );
        }

        _prefetchCount = prefetchCount;
        return this;
    }

    /// <summary>
    /// Sets the consumer dispatch concurrency (the number of deliveries dispatched in parallel per channel).
    /// </summary>
    /// <param name="consumerDispatchConcurrency">The dispatch concurrency; must be greater than zero.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="consumerDispatchConcurrency" /> is zero.</exception>
    public RabbitMqInboundConsumerBuilder Concurrency(ushort consumerDispatchConcurrency)
    {
        if (consumerDispatchConcurrency == 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(consumerDispatchConcurrency),
                consumerDispatchConcurrency,
                "The value must be greater than zero."
            );
        }

        _consumerDispatchConcurrency = consumerDispatchConcurrency;
        return this;
    }

    /// <summary>
    /// Sets the number of channels the consumer spreads its deliveries across.
    /// </summary>
    /// <param name="channelCount">The channel count; must be greater than zero.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="channelCount" /> is less than one.</exception>
    public RabbitMqInboundConsumerBuilder ChannelCount(int channelCount)
    {
        if (channelCount < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(channelCount),
                channelCount,
                "The value must be greater than zero."
            );
        }

        _channelCount = channelCount;
        return this;
    }

    /// <summary>
    /// Consumes through the named inbound channel group instead of an implicit per-consumer group.
    /// </summary>
    /// <param name="channelGroupName">The name of the channel group to use.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="channelGroupName" /> is null or whitespace.</exception>
    public RabbitMqInboundConsumerBuilder UseChannelGroup(string channelGroupName)
    {
        _channelGroupName = RequireText(channelGroupName, nameof(channelGroupName));
        return this;
    }

    /// <summary>
    /// Overrides the inbound message inspector with <typeparamref name="TInspector" /> instead of the default
    /// CloudEvents inspector.
    /// </summary>
    /// <typeparam name="TInspector">The inspector type to use.</typeparam>
    /// <returns>The same builder for chaining.</returns>
    public RabbitMqInboundConsumerBuilder UseInspector<TInspector>()
        where TInspector : class, IInboundMessageInspector
    {
        _inspectorType = typeof(TInspector);
        return this;
    }

    /// <summary>
    /// Uses RabbitMQ.Client's pooled delivery buffer directly instead of copying the message body.
    /// </summary>
    /// <remarks>
    /// The transport message body and any value derived from it without copying, including
    /// <see cref="CloudEventEnvelope.Data" />, are valid only until the message handler completes. The message must
    /// not be retained and processing must not be offloaded past the handler's lifetime. Violations read reused
    /// buffer contents rather than throwing.
    /// </remarks>
    public RabbitMqInboundConsumerBuilder ZeroCopyBody()
    {
        _copyBody = false;
        return this;
    }

    /// <summary>
    /// Adds a handler for <typeparamref name="TMessage" />. The concrete <typeparamref name="THandler" /> type is
    /// auto-registered as scoped and resolved from the per-delivery scope. Register the concrete handler type before
    /// calling <c>AddRabbitMq*Topology</c> to choose a different lifetime; auto-registration yields to an existing
    /// registration. Use <paramref name="configure" /> to configure the deserializer and acknowledgement mode for
    /// this handler.
    /// </summary>
    /// <param name="configure">An optional callback that configures this handler.</param>
    public RabbitMqInboundConsumerBuilder Handle<TMessage, THandler>(
        Action<RabbitMqInboundHandlerBuilder>? configure = null
    )
        where THandler : class, IMessageHandler<TMessage>
    {
        return HandleNamed<TMessage, THandler>(endpointName: null, configure);
    }

    /// <summary>
    /// Adds a named handler for <typeparamref name="TMessage" />. The concrete <typeparamref name="THandler" /> type
    /// is auto-registered as scoped and resolved from the per-delivery scope. Register the concrete handler type
    /// before calling <c>AddRabbitMq*Topology</c> to choose a different lifetime; auto-registration yields to an
    /// existing registration. Use <paramref name="configure" /> to configure the deserializer and acknowledgement
    /// mode for this handler.
    /// </summary>
    /// <param name="endpointName">The optional endpoint name.</param>
    /// <param name="configure">An optional callback that configures this handler.</param>
    public RabbitMqInboundConsumerBuilder HandleNamed<TMessage, THandler>(
        string? endpointName,
        Action<RabbitMqInboundHandlerBuilder>? configure = null
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

        var handlerBuilder = new RabbitMqInboundHandlerBuilder();
        configure?.Invoke(handlerBuilder);
        var (deserializerType, ackMode) = handlerBuilder.Build();

        _handlers.Add(
            new RabbitMqInboundHandlerDefinition(
                endpointName,
                typeof(TMessage),
                typeof(THandler),
                MessageHandlerInvocation.Create<TMessage, THandler>(),
                deserializerType,
                ackMode
            )
        );
        return this;
    }

    internal RabbitMqInboundConsumerDefinition Build()
    {
        return new RabbitMqInboundConsumerDefinition(
            _queueName,
            _inspectorType,
            _channelGroupName,
            _channelCount,
            _prefetchCount,
            _consumerDispatchConcurrency,
            _copyBody,
            _handlers.AsReadOnly()
        );
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
