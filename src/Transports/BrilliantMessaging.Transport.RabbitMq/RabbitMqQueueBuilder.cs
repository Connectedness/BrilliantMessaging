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

    /// <summary>
    /// Sets the delivery limit (<c>x-delivery-limit</c>) for a quorum queue, after which a message is
    /// dead-lettered instead of redelivered.
    /// </summary>
    /// <param name="limit">
    /// The maximum number of deliveries; must be <c>-1</c> or greater. <c>-1</c> disables the
    /// limit (not recommended).
    /// </param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="limit" /> is less than <c>-1</c>.</exception>
    public RabbitMqQueueBuilder WithDeliveryLimit(int limit)
    {
        if (limit < -1)
        {
            throw new ArgumentOutOfRangeException(nameof(limit), "The value must be -1 or greater.");
        }

        _arguments["x-delivery-limit"] = limit;
        return this;
    }

    /// <summary>
    /// Sets the delayed-retry configuration (<c>x-delayed-retry-type</c>, <c>x-delayed-retry-min</c>, and
    /// optionally <c>x-delayed-retry-max</c>) for a quorum queue, controlling which redelivered messages are
    /// held back for a delay before being retried.
    /// </summary>
    /// <param name="type">The delayed-retry type.</param>
    /// <param name="minDelay">The minimum delay before a retry; must be zero or greater.</param>
    /// <param name="maxDelay">The optional maximum delay before a retry; must be zero or greater when supplied.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// Thrown when <paramref name="minDelay" /> is negative, or when <paramref name="maxDelay" /> is supplied and
    /// negative.
    /// </exception>
    public RabbitMqQueueBuilder WithDelayedRetry(
        RabbitMqDelayedRetryType type,
        TimeSpan minDelay,
        TimeSpan? maxDelay = null
    )
    {
        _arguments["x-delayed-retry-type"] = MapDelayedRetryType(type);
        _arguments["x-delayed-retry-min"] = ToMilliseconds(minDelay, nameof(minDelay));

        if (maxDelay is { } maxDelayValue)
        {
            _arguments["x-delayed-retry-max"] = ToMilliseconds(maxDelayValue, nameof(maxDelay));
        }
        else
        {
            _arguments.Remove("x-delayed-retry-max");
        }

        return this;
    }

    /// <summary>
    /// Sets the delayed-retry configuration for a quorum queue with the type defaulting to
    /// <see cref="RabbitMqDelayedRetryType.All" />.
    /// </summary>
    /// <param name="minDelay">The minimum delay before a retry; must be zero or greater.</param>
    /// <param name="maxDelay">The optional maximum delay before a retry; must be zero or greater when supplied.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// Thrown when <paramref name="minDelay" /> is negative, or when <paramref name="maxDelay" /> is supplied and
    /// negative.
    /// </exception>
    public RabbitMqQueueBuilder WithDelayedRetry(TimeSpan minDelay, TimeSpan? maxDelay = null)
    {
        return WithDelayedRetry(RabbitMqDelayedRetryType.All, minDelay, maxDelay);
    }

    /// <summary>
    /// Sets the dead-lettering strategy (<c>x-dead-letter-strategy</c>) for a quorum queue, controlling whether
    /// dead-lettered messages can be delivered more than once to the dead-letter exchange.
    /// </summary>
    /// <param name="strategy">The dead-lettering strategy.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <remarks>
    /// <see cref="RabbitMqDeadLetterStrategy.AtLeastOnce" /> additionally requires
    /// <see cref="WithOverflow(RabbitMqOverflow)" /> set to <see cref="RabbitMqOverflow.RejectPublish" /> at
    /// runtime; this constraint is enforced by the broker, not by the compiler.
    /// </remarks>
    public RabbitMqQueueBuilder WithDeadLetterStrategy(RabbitMqDeadLetterStrategy strategy)
    {
        _arguments["x-dead-letter-strategy"] = MapDeadLetterStrategy(strategy);
        return this;
    }

    /// <summary>
    /// Sets the overflow behavior (<c>x-overflow</c>) of the queue, controlling what happens when the maximum
    /// length or length-bytes limit is reached.
    /// </summary>
    /// <param name="overflow">The overflow behavior.</param>
    /// <returns>The same builder for chaining.</returns>
    public RabbitMqQueueBuilder WithOverflow(RabbitMqOverflow overflow)
    {
        _arguments["x-overflow"] = MapOverflow(overflow);
        return this;
    }

    /// <summary>
    /// Sets the maximum priority (<c>x-max-priority</c>) of a classic queue, defining the range of message
    /// priorities the queue supports.
    /// </summary>
    /// <param name="maxPriority">The maximum priority; must be greater than zero (1-255).</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="maxPriority" /> is zero.</exception>
    /// <remarks>
    /// Quorum queues silently ignore <c>x-max-priority</c> and always use the full 0-31 priority range; the
    /// compiler rejects this argument on quorum queues. Use <see cref="AsClassicQueue" /> to control the priority
    /// range.
    /// </remarks>
    public RabbitMqQueueBuilder WithMaxPriority(byte maxPriority)
    {
        if (maxPriority == 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxPriority), "The value must be greater than zero.");
        }

        _arguments["x-max-priority"] = maxPriority;
        return this;
    }

    /// <summary>
    /// Sets the queue leader locator (<c>x-queue-leader-locator</c>) for a quorum queue, controlling how the
    /// leader for a new quorum queue is selected across the cluster.
    /// </summary>
    /// <param name="locator">The leader locator strategy.</param>
    /// <returns>The same builder for chaining.</returns>
    public RabbitMqQueueBuilder WithQueueLeaderLocator(RabbitMqQueueLeaderLocator locator)
    {
        _arguments["x-queue-leader-locator"] = MapQueueLeaderLocator(locator);
        return this;
    }

    /// <summary>
    /// Sets the initial quorum cluster size (<c>x-quorum-initial-group-size</c>) for a quorum queue, controlling
    /// the number of replicas in the quorum group at declaration time.
    /// </summary>
    /// <param name="size">The initial cluster size; must be one or greater.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="size" /> is less than one.</exception>
    public RabbitMqQueueBuilder WithInitialClusterSize(int size)
    {
        if (size < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(size), "The value must be one or greater.");
        }

        _arguments["x-quorum-initial-group-size"] = size;
        return this;
    }

    /// <summary>
    /// Sets the consumer timeout (<c>x-consumer-timeout</c>) for a quorum queue, after which a consumer is
    /// considered timed out and the message is requeued or dead-lettered.
    /// </summary>
    /// <param name="timeout">The consumer timeout; must be greater than zero.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="timeout" /> is zero or negative.</exception>
    public RabbitMqQueueBuilder WithConsumerTimeout(TimeSpan timeout)
    {
        if (timeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(timeout), "The value must be greater than zero.");
        }

        _arguments["x-consumer-timeout"] = checked((long) timeout.TotalMilliseconds);
        return this;
    }

    private static string MapDelayedRetryType(RabbitMqDelayedRetryType type)
    {
        return type switch
        {
            RabbitMqDelayedRetryType.Disabled => "disabled",
            RabbitMqDelayedRetryType.All => "all",
            RabbitMqDelayedRetryType.Failed => "failed",
            RabbitMqDelayedRetryType.Returned => "returned",
            _ => throw new ArgumentOutOfRangeException(nameof(type), type, "Unsupported delayed retry type.")
        };
    }

    private static string MapDeadLetterStrategy(RabbitMqDeadLetterStrategy strategy)
    {
        return strategy switch
        {
            RabbitMqDeadLetterStrategy.AtMostOnce => "at-most-once",
            RabbitMqDeadLetterStrategy.AtLeastOnce => "at-least-once",
            _ => throw new ArgumentOutOfRangeException(nameof(strategy), strategy, "Unsupported dead-letter strategy.")
        };
    }

    private static string MapOverflow(RabbitMqOverflow overflow)
    {
        return overflow switch
        {
            RabbitMqOverflow.DropHead => "drop-head",
            RabbitMqOverflow.RejectPublish => "reject-publish",
            RabbitMqOverflow.RejectPublishDlx => "reject-publish-dlx",
            _ => throw new ArgumentOutOfRangeException(nameof(overflow), overflow, "Unsupported overflow behavior.")
        };
    }

    private static string MapQueueLeaderLocator(RabbitMqQueueLeaderLocator locator)
    {
        return locator switch
        {
            RabbitMqQueueLeaderLocator.ClientLocal => "client-local",
            RabbitMqQueueLeaderLocator.Balanced => "balanced",
            _ => throw new ArgumentOutOfRangeException(nameof(locator), locator, "Unsupported queue leader locator.")
        };
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
