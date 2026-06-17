using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using RabbitMQ.Client;

namespace Bmf.Transport.RabbitMq;

/// <summary>
/// Fluent builder for a RabbitMQ exchange declaration. It collects the exchange name, type, declare mode,
/// durability flags, and broker arguments into a <see cref="RabbitMqExchangeDefinition" />.
/// </summary>
public sealed class RabbitMqExchangeBuilder
{
    private readonly Dictionary<string, object?> _arguments = new (StringComparer.Ordinal);

    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMqExchangeBuilder" /> class for a durable, actively
    /// declared exchange.
    /// </summary>
    /// <param name="name">The exchange name.</param>
    /// <param name="type">The exchange type (for example <c>direct</c>, <c>topic</c>, <c>fanout</c>, or <c>headers</c>).</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="name" /> or <paramref name="type" /> is null or whitespace.</exception>
    public RabbitMqExchangeBuilder(string name, string type)
    {
        Name = RequireText(name, nameof(name));
        Type = RequireText(type, nameof(type));
        DeclareMode = RabbitMqDeclareMode.Active;
        Durable = true;
    }

    /// <summary>
    /// Gets the exchange name.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Gets the exchange type.
    /// </summary>
    public string Type { get; }

    /// <summary>
    /// Gets the declare mode that controls whether and how the exchange is declared at provisioning time.
    /// </summary>
    public RabbitMqDeclareMode DeclareMode { get; private set; }

    /// <summary>
    /// Gets a value indicating whether the exchange survives a broker restart.
    /// </summary>
    public bool Durable { get; private set; }

    /// <summary>
    /// Gets a value indicating whether the exchange is deleted automatically once the last queue is unbound.
    /// </summary>
    public bool AutoDelete { get; private set; }

    /// <summary>
    /// Sets the declare mode for the exchange.
    /// </summary>
    /// <param name="declareMode">The declare mode to use.</param>
    /// <returns>The same builder for chaining.</returns>
    public RabbitMqExchangeBuilder WithDeclareMode(RabbitMqDeclareMode declareMode)
    {
        DeclareMode = declareMode;
        return this;
    }

    /// <summary>
    /// Sets whether the exchange is durable (survives a broker restart).
    /// </summary>
    /// <param name="durable"><see langword="true" /> to make the exchange durable.</param>
    /// <returns>The same builder for chaining.</returns>
    public RabbitMqExchangeBuilder DurableExchange(bool durable = true)
    {
        Durable = durable;
        return this;
    }

    /// <summary>
    /// Sets whether the exchange is auto-deleted once the last queue is unbound.
    /// </summary>
    /// <param name="autoDelete"><see langword="true" /> to make the exchange auto-delete.</param>
    /// <returns>The same builder for chaining.</returns>
    public RabbitMqExchangeBuilder AutoDeleteExchange(bool autoDelete = true)
    {
        AutoDelete = autoDelete;
        return this;
    }

    /// <summary>
    /// Adds an arbitrary declaration argument to the exchange.
    /// </summary>
    /// <param name="name">The argument name.</param>
    /// <param name="value">The argument value.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="name" /> is null or whitespace.</exception>
    public RabbitMqExchangeBuilder WithArgument(string name, object? value)
    {
        _arguments[RequireText(name, nameof(name))] = value;
        return this;
    }

    /// <summary>
    /// Sets the alternate exchange (<c>alternate-exchange</c>) that unroutable messages are forwarded to.
    /// </summary>
    /// <param name="exchangeName">The alternate exchange name.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="exchangeName" /> is null or whitespace.</exception>
    public RabbitMqExchangeBuilder WithAlternateExchange(string exchangeName)
    {
        _arguments["alternate-exchange"] = RequireText(exchangeName, nameof(exchangeName));
        return this;
    }

    /// <summary>
    /// Configures the exchange as a delayed-message exchange (<c>x-delayed-type</c>), delegating actual routing to
    /// the given underlying exchange type. Requires the delayed-message-exchange plugin on the broker.
    /// </summary>
    /// <param name="delayedExchangeType">The underlying exchange type used once the delay elapses; defaults to <c>direct</c>.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="delayedExchangeType" /> is null or whitespace.</exception>
    public RabbitMqExchangeBuilder AsDelayedMessageExchange(string delayedExchangeType = ExchangeType.Direct)
    {
        _arguments["x-delayed-type"] = RequireText(delayedExchangeType, nameof(delayedExchangeType));
        return this;
    }

    /// <summary>
    /// Builds the immutable <see cref="RabbitMqExchangeDefinition" /> from the configured values.
    /// </summary>
    /// <returns>The exchange definition.</returns>
    public RabbitMqExchangeDefinition Build()
    {
        return new RabbitMqExchangeDefinition(
            Name,
            Type,
            DeclareMode,
            Durable,
            AutoDelete,
            new ReadOnlyDictionary<string, object?>(_arguments)
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
