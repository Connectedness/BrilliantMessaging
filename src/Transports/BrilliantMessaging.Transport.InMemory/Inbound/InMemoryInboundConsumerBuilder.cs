using System;
using System.Collections.Immutable;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;

namespace BrilliantMessaging.Transport.InMemory.Inbound;

/// <summary>
/// Fluent builder for an in-memory consumer on a single declared topic. It configures the worker concurrency and
/// failure handling and registers one or more typed handlers.
/// </summary>
public sealed class InMemoryInboundConsumerBuilder : IBuildable<InMemoryInboundConsumerDefinition>
{
    private readonly ImmutableArray<InMemoryInboundHandlerDefinition>.Builder _handlers =
        ImmutableArray.CreateBuilder<InMemoryInboundHandlerDefinition>();

    private readonly string _topic;
    private int _concurrency = 1;
    private InMemoryDeliveryPolicy _deliveryPolicy = InMemoryDeliveryPolicy.Drop;

    /// <summary>
    /// Initializes a new instance of the <see cref="InMemoryInboundConsumerBuilder" /> class for the given topic.
    /// </summary>
    /// <param name="topic">The declared topic to consume.</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="topic" /> is null or whitespace.</exception>
    public InMemoryInboundConsumerBuilder(string topic)
    {
        _topic = RequireText(topic, nameof(topic));
    }

    /// <inheritdoc />
    InMemoryInboundConsumerDefinition IBuildable<InMemoryInboundConsumerDefinition>.Build()
    {
        return new InMemoryInboundConsumerDefinition(
            _topic,
            _concurrency,
            _handlers.ToImmutable(),
            _deliveryPolicy
        );
    }

    /// <summary>
    /// Sets the number of background workers that process this consumer's deliveries. The default of <c>1</c>
    /// preserves single-worker FIFO delivery; a higher value processes deliveries in parallel and relaxes
    /// ordering.
    /// </summary>
    /// <param name="concurrency">The worker count; must be greater than zero.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="concurrency" /> is less than one.</exception>
    public InMemoryInboundConsumerBuilder Concurrency(int concurrency)
    {
        if (concurrency < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(concurrency),
                concurrency,
                "The value must be greater than zero."
            );
        }

        _concurrency = concurrency;
        return this;
    }

    /// <summary>
    /// Configures how this consumer handles delivery failures. Without a call the consumer drops failed
    /// deliveries.
    /// </summary>
    /// <param name="configure">A callback that configures the retry and dead-letter behaviour.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="configure" /> is <see langword="null" />.</exception>
    public InMemoryInboundConsumerBuilder OnFailure(Action<InMemoryDeliveryPolicyBuilder> configure)
    {
        if (configure is null)
        {
            throw new ArgumentNullException(nameof(configure));
        }

        InMemoryDeliveryPolicyBuilder builder = new ();
        configure(builder);
        _deliveryPolicy = ((IBuildable<InMemoryDeliveryPolicy>) builder).Build();
        return this;
    }

    /// <summary>
    /// Adds a handler for <typeparamref name="TMessage" />. The concrete <typeparamref name="THandler" /> type is
    /// auto-registered as scoped and resolved from the per-delivery scope. Register the concrete handler type
    /// before calling <c>AddInMemory*Topology</c> to choose a different lifetime; auto-registration yields to an
    /// existing registration.
    /// </summary>
    /// <param name="configure">An optional callback that configures this handler.</param>
    /// <typeparam name="TMessage">The message type the handler processes.</typeparam>
    /// <typeparam name="THandler">The concrete handler type.</typeparam>
    /// <returns>The same builder for chaining.</returns>
    public InMemoryInboundConsumerBuilder Handle<TMessage, THandler>(
        Action<InMemoryInboundHandlerBuilder>? configure = null
    )
        where THandler : class, IMessageHandler<TMessage>
    {
        return HandleNamed<TMessage, THandler>(endpointName: null, configure);
    }

    /// <summary>
    /// Adds a named handler for <typeparamref name="TMessage" />. The concrete <typeparamref name="THandler" />
    /// type is auto-registered as scoped and resolved from the per-delivery scope.
    /// </summary>
    /// <param name="endpointName">The optional endpoint name.</param>
    /// <param name="configure">An optional callback that configures this handler.</param>
    /// <typeparam name="TMessage">The message type the handler processes.</typeparam>
    /// <typeparam name="THandler">The concrete handler type.</typeparam>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentException">Thrown when <typeparamref name="THandler" /> is not a concrete class.</exception>
    public InMemoryInboundConsumerBuilder HandleNamed<TMessage, THandler>(
        string? endpointName,
        Action<InMemoryInboundHandlerBuilder>? configure = null
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

        var handlerBuilder = new InMemoryInboundHandlerBuilder();
        configure?.Invoke(handlerBuilder);
        var handlerConfiguration = ((IBuildable<InMemoryInboundHandlerConfiguration>) handlerBuilder).Build();

        _handlers.Add(
            new InMemoryInboundHandlerDefinition(
                endpointName,
                typeof(TMessage),
                typeof(THandler),
                MessageHandlerInvocation.Create<TMessage, THandler>(),
                handlerConfiguration.DeserializerType,
                handlerConfiguration.AckMode
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
