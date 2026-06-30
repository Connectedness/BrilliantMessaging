using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Transport.InMemory.Inbound;
using BrilliantMessaging.Transport.InMemory.Outbound;

namespace BrilliantMessaging.Transport.InMemory;

/// <summary>
/// Fluent builder for an in-memory topology. It declares topics, maps outbound message types to topics, and
/// registers inbound consumers, then compiles them into an <see cref="InMemoryTopologyConfiguration" />. The same
/// builder backs the unified, outbound-only, and inbound-only registration entry points.
/// </summary>
public sealed class InMemoryTopologyBuilder
    : IInMemoryOutboundTopologyBuilder, IInMemoryInboundTopologyBuilder, IBuildable<InMemoryTopologyConfiguration>
{
    /// <summary>
    /// The default graceful shutdown timeout applied when the builder does not configure one.
    /// </summary>
    public static readonly TimeSpan DefaultShutdownTimeout = TimeSpan.FromSeconds(30);

    private readonly ImmutableArray<InMemoryInboundConsumerDefinition>.Builder _consumers =
        ImmutableArray.CreateBuilder<InMemoryInboundConsumerDefinition>();

    private readonly ImmutableArray<InMemoryOutboundTargetDefinition>.Builder _targets =
        ImmutableArray.CreateBuilder<InMemoryOutboundTargetDefinition>();

    private readonly ImmutableArray<string>.Builder _topics = ImmutableArray.CreateBuilder<string>();
    private readonly HashSet<string> _topicSet = new (StringComparer.Ordinal);

    private TimeSpan _shutdownTimeout = DefaultShutdownTimeout;

    /// <inheritdoc />
    InMemoryTopologyConfiguration IBuildable<InMemoryTopologyConfiguration>.Build()
    {
        return new InMemoryTopologyConfiguration(
            _topics.ToImmutable(),
            _targets.ToImmutable(),
            _consumers.ToImmutable(),
            _shutdownTimeout
        );
    }

    IInMemoryInboundTopologyBuilder IInMemoryInboundTopologyBuilder.Topic(string topic)
    {
        return Topic(topic);
    }

    IInMemoryInboundTopologyBuilder IInMemoryInboundTopologyBuilder.Consume(
        string topic,
        Action<InMemoryInboundConsumerBuilder> configure
    )
    {
        return Consume(topic, configure);
    }

    IInMemoryInboundTopologyBuilder IInMemoryInboundTopologyBuilder.ShutdownTimeout(TimeSpan timeout)
    {
        return ShutdownTimeout(timeout);
    }

    IInMemoryOutboundTopologyBuilder IInMemoryOutboundTopologyBuilder.Topic(string topic)
    {
        return Topic(topic);
    }

    IInMemoryOutboundTopologyBuilder IInMemoryOutboundTopologyBuilder.Publish<TMessage>(
        Action<InMemoryOutboundTargetBuilder<TMessage>> configure
    )
    {
        return Publish(configure);
    }

    IInMemoryOutboundTopologyBuilder IInMemoryOutboundTopologyBuilder.ShutdownTimeout(TimeSpan timeout)
    {
        return ShutdownTimeout(timeout);
    }

    /// <summary>
    /// Declares a topic resource. Topics referenced by <c>ToTopic</c> and <c>Consume</c> must be declared.
    /// </summary>
    /// <param name="topic">The topic name to declare.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="topic" /> is null or whitespace.</exception>
    /// <exception cref="InvalidOperationException">Thrown when <paramref name="topic" /> is already declared.</exception>
    public InMemoryTopologyBuilder Topic(string topic)
    {
        if (string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(topic));
        }

        if (!_topicSet.Add(topic))
        {
            throw new InvalidOperationException($"Topic '{topic}' is already declared.");
        }

        _topics.Add(topic);
        return this;
    }

    /// <summary>
    /// Maps outbound messages of type <typeparamref name="TMessage" /> to a declared topic.
    /// </summary>
    /// <param name="configure">A callback that configures the outbound target.</param>
    /// <typeparam name="TMessage">The message type the target publishes.</typeparam>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="configure" /> is <see langword="null" />.</exception>
    public InMemoryTopologyBuilder Publish<TMessage>(Action<InMemoryOutboundTargetBuilder<TMessage>> configure)
    {
        if (configure is null)
        {
            throw new ArgumentNullException(nameof(configure));
        }

        InMemoryOutboundTargetBuilder<TMessage> targetBuilder = new ();
        configure(targetBuilder);
        _targets.Add(((IBuildable<InMemoryOutboundTargetDefinition>) targetBuilder).Build());
        return this;
    }

    /// <summary>
    /// Consumes messages from a declared topic. Each call adds an independent consumer route; several routes on the
    /// same topic each receive a fanout copy of every published message.
    /// </summary>
    /// <param name="topic">The declared topic to consume from.</param>
    /// <param name="configure">A callback that configures the consumer.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="topic" /> is null or whitespace.</exception>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="configure" /> is <see langword="null" />.</exception>
    public InMemoryTopologyBuilder Consume(string topic, Action<InMemoryInboundConsumerBuilder> configure)
    {
        if (string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(topic));
        }

        if (configure is null)
        {
            throw new ArgumentNullException(nameof(configure));
        }

        InMemoryInboundConsumerBuilder consumerBuilder = new (topic);
        configure(consumerBuilder);
        _consumers.Add(((IBuildable<InMemoryInboundConsumerDefinition>) consumerBuilder).Build());
        return this;
    }

    /// <summary>
    /// Sets the graceful shutdown timeout the runtime uses to drain in-flight deliveries before cancelling them.
    /// </summary>
    /// <param name="timeout">The shutdown timeout; must be positive or <see cref="Timeout.InfiniteTimeSpan" />.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="timeout" /> is not positive and not infinite.</exception>
    public InMemoryTopologyBuilder ShutdownTimeout(TimeSpan timeout)
    {
        if (timeout != Timeout.InfiniteTimeSpan && timeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(timeout),
                timeout,
                "The value must be positive or Timeout.InfiniteTimeSpan."
            );
        }

        _shutdownTimeout = timeout;
        return this;
    }
}
