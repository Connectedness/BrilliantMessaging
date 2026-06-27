using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using BrilliantMessaging.Core.Messaging;

namespace BrilliantMessaging.Transport.RabbitMq;

/// <summary>
/// Fluent builder for a RabbitMQ queue declaration. It collects the queue name, declare mode, durability flags,
/// and broker arguments (dead-lettering, TTLs, length limits, queue type) into a <see cref="RabbitMqQueueDefinition" />.
/// </summary>
public sealed class RabbitMqQueueBuilder : IBuildable<RabbitMqQueueDefinition>
{
    private readonly Dictionary<string, object?> _arguments = new (StringComparer.Ordinal);

    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMqQueueBuilder" /> class for a durable, actively
    /// declared queue.
    /// </summary>
    /// <param name="name">The queue name.</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="name" /> is null or whitespace.</exception>
    public RabbitMqQueueBuilder(string name)
    {
        Name = RequireText(name, nameof(name));
        DeclareMode = RabbitMqDeclareMode.Active;
        Durable = true;
        _arguments["x-queue-type"] = "quorum";
    }

    /// <summary>
    /// Gets the queue name.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Gets the declare mode that controls whether and how the queue is declared at provisioning time.
    /// </summary>
    public RabbitMqDeclareMode DeclareMode { get; private set; }

    /// <summary>
    /// Gets a value indicating whether the queue survives a broker restart.
    /// </summary>
    public bool Durable { get; private set; }

    /// <summary>
    /// Gets a value indicating whether the queue is exclusive to its declaring connection.
    /// </summary>
    public bool Exclusive { get; private set; }

    /// <summary>
    /// Gets a value indicating whether the queue is deleted automatically once its last consumer disconnects.
    /// </summary>
    public bool AutoDelete { get; private set; }

    /// <inheritdoc />
    RabbitMqQueueDefinition IBuildable<RabbitMqQueueDefinition>.Build()
    {
        return new RabbitMqQueueDefinition(
            Name,
            DeclareMode,
            Durable,
            Exclusive,
            AutoDelete,
            new ReadOnlyDictionary<string, object?>(_arguments)
        );
    }

    /// <summary>
    /// Sets the declare mode for the queue.
    /// </summary>
    /// <param name="declareMode">The declare mode to use.</param>
    /// <returns>The same builder for chaining.</returns>
    public RabbitMqQueueBuilder WithDeclareMode(RabbitMqDeclareMode declareMode)
    {
        DeclareMode = declareMode;
        return this;
    }

    /// <summary>
    /// Sets whether the queue is durable (survives a broker restart).
    /// </summary>
    /// <param name="durable"><see langword="true" /> to make the queue durable.</param>
    /// <returns>The same builder for chaining.</returns>
    public RabbitMqQueueBuilder DurableQueue(bool durable = true)
    {
        Durable = durable;
        return this;
    }

    /// <summary>
    /// Sets whether the queue is exclusive to its declaring connection.
    /// </summary>
    /// <param name="exclusive"><see langword="true" /> to make the queue exclusive.</param>
    /// <returns>The same builder for chaining.</returns>
    public RabbitMqQueueBuilder ExclusiveQueue(bool exclusive = true)
    {
        Exclusive = exclusive;
        return this;
    }

    /// <summary>
    /// Sets whether the queue is auto-deleted once its last consumer disconnects.
    /// </summary>
    /// <param name="autoDelete"><see langword="true" /> to make the queue auto-delete.</param>
    /// <returns>The same builder for chaining.</returns>
    public RabbitMqQueueBuilder AutoDeleteQueue(bool autoDelete = true)
    {
        AutoDelete = autoDelete;
        return this;
    }

    /// <summary>
    /// Adds an arbitrary declaration argument (an <c>x-</c> argument) to the queue.
    /// </summary>
    /// <param name="name">The argument name.</param>
    /// <param name="value">The argument value.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="name" /> is null or whitespace.</exception>
    public RabbitMqQueueBuilder WithArgument(string name, object? value)
    {
        _arguments[RequireText(name, nameof(name))] = value;
        return this;
    }

    /// <summary>
    /// Sets the dead-letter exchange (<c>x-dead-letter-exchange</c>) that rejected or expired messages are routed to.
    /// </summary>
    /// <param name="exchangeName">The dead-letter exchange name.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="exchangeName" /> is null or whitespace.</exception>
    public RabbitMqQueueBuilder WithDeadLetterExchange(string exchangeName)
    {
        _arguments["x-dead-letter-exchange"] = RequireText(exchangeName, nameof(exchangeName));
        return this;
    }

    /// <summary>
    /// Sets the dead-letter routing key (<c>x-dead-letter-routing-key</c>) used when dead-lettering messages.
    /// </summary>
    /// <param name="routingKey">The dead-letter routing key; an empty string is permitted.</param>
    /// <returns>The same builder for chaining.</returns>
    public RabbitMqQueueBuilder WithDeadLetterRoutingKey(string routingKey)
    {
        _arguments["x-dead-letter-routing-key"] = routingKey ?? string.Empty;
        return this;
    }

    /// <summary>
    /// Sets the per-queue message time to live (<c>x-message-ttl</c>).
    /// </summary>
    /// <param name="timeToLive">The message time to live; must be zero or greater.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="timeToLive" /> is negative.</exception>
    public RabbitMqQueueBuilder WithMessageTtl(TimeSpan timeToLive)
    {
        _arguments["x-message-ttl"] = ToMilliseconds(timeToLive, nameof(timeToLive));
        return this;
    }

    /// <summary>
    /// Sets the queue expiry (<c>x-expires</c>), after which an unused queue is deleted.
    /// </summary>
    /// <param name="expires">The unused-queue expiry; must be zero or greater.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="expires" /> is negative.</exception>
    public RabbitMqQueueBuilder WithExpires(TimeSpan expires)
    {
        _arguments["x-expires"] = ToMilliseconds(expires, nameof(expires));
        return this;
    }

    /// <summary>
    /// Sets the maximum number of messages the queue retains (<c>x-max-length</c>).
    /// </summary>
    /// <param name="maxLength">The maximum message count; must be zero or greater.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="maxLength" /> is negative.</exception>
    public RabbitMqQueueBuilder WithMaxLength(long maxLength)
    {
        if (maxLength < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxLength), "The value must be zero or greater.");
        }

        _arguments["x-max-length"] = maxLength;
        return this;
    }

    /// <summary>
    /// Sets the maximum total body size the queue retains (<c>x-max-length-bytes</c>).
    /// </summary>
    /// <param name="maxLengthBytes">The maximum total body size in bytes; must be zero or greater.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="maxLengthBytes" /> is negative.</exception>
    public RabbitMqQueueBuilder WithMaxLengthBytes(long maxLengthBytes)
    {
        if (maxLengthBytes < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxLengthBytes), "The value must be zero or greater.");
        }

        _arguments["x-max-length-bytes"] = maxLengthBytes;
        return this;
    }

    /// <summary>
    /// Sets the queue type (<c>x-queue-type</c>), for example <c>classic</c> or <c>quorum</c>.
    /// </summary>
    /// <param name="queueType">The queue type.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="queueType" /> is null or whitespace.</exception>
    public RabbitMqQueueBuilder WithQueueType(string queueType)
    {
        _arguments["x-queue-type"] = RequireText(queueType, nameof(queueType));
        return this;
    }

    /// <summary>
    /// Declares the queue as a quorum queue (<c>x-queue-type = quorum</c>).
    /// </summary>
    /// <returns>The same builder for chaining.</returns>
    public RabbitMqQueueBuilder AsQuorumQueue()
    {
        _arguments["x-queue-type"] = "quorum";
        return this;
    }

    /// <summary>
    /// Declares the queue as a classic queue (<c>x-queue-type = classic</c>).
    /// </summary>
    /// <returns>The same builder for chaining.</returns>
    public RabbitMqQueueBuilder AsClassicQueue()
    {
        _arguments["x-queue-type"] = "classic";
        return this;
    }

    /// <summary>
    /// Declares the queue using the broker's configured default queue type
    /// (<c>x-queue-type</c> is not set).
    /// </summary>
    /// <returns>The same builder for chaining.</returns>
    /// <remarks>
    /// Because the queue type is not declared, the topology compiler cannot detect it, so not all
    /// queue features and configurations (such as quorum-queue redelivery handling) are available.
    /// Only use this method when relying on the broker's default is required; otherwise prefer
    /// explicitly calling <see cref="AsQuorumQueue" /> or <see cref="AsClassicQueue" />.
    /// </remarks>
    public RabbitMqQueueBuilder UseDefaultQueueType()
    {
        _arguments.Remove("x-queue-type");
        return this;
    }

    /// <summary>
    /// Enables single active consumer mode (<c>x-single-active-consumer</c>), so only one consumer processes the
    /// queue at a time.
    /// </summary>
    /// <param name="singleActiveConsumer"><see langword="true" /> to enable single active consumer mode.</param>
    /// <returns>The same builder for chaining.</returns>
    public RabbitMqQueueBuilder SingleActiveConsumer(bool singleActiveConsumer = true)
    {
        _arguments["x-single-active-consumer"] = singleActiveConsumer;
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

    private static long ToMilliseconds(TimeSpan value, string parameterName)
    {
        if (value < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(parameterName, "The value must be zero or greater.");
        }

        return checked((long) value.TotalMilliseconds);
    }
}
