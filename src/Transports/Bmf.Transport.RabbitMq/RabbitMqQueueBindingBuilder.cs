using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace Bmf.Transport.RabbitMq;

/// <summary>
/// Fluent builder for a binding from a source exchange to a queue, collecting the routing key, binding mode, and
/// binding arguments into a <see cref="RabbitMqQueueBindingDefinition" />.
/// </summary>
public sealed class RabbitMqQueueBindingBuilder
{
    private readonly Dictionary<string, object?> _arguments = new (StringComparer.Ordinal);

    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMqQueueBindingBuilder" /> class.
    /// </summary>
    /// <param name="exchangeName">The name of the source exchange.</param>
    /// <param name="queueName">The name of the bound queue.</param>
    /// <param name="routingKey">The binding routing key; an empty string is permitted.</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="exchangeName" /> or <paramref name="queueName" /> is null or whitespace.</exception>
    public RabbitMqQueueBindingBuilder(string exchangeName, string queueName, string routingKey)
    {
        SourceExchangeName = RequireText(exchangeName, nameof(exchangeName));
        QueueName = RequireText(queueName, nameof(queueName));
        RoutingKey = routingKey ?? string.Empty;
        BindingMode = RabbitMqBindingMode.Active;
    }

    /// <summary>
    /// Gets the name of the source exchange.
    /// </summary>
    public string SourceExchangeName { get; }

    /// <summary>
    /// Gets the name of the bound queue.
    /// </summary>
    public string QueueName { get; }

    /// <summary>
    /// Gets the binding routing key.
    /// </summary>
    public string RoutingKey { get; }

    /// <summary>
    /// Gets the binding mode that controls whether and how the binding is declared at provisioning time.
    /// </summary>
    public RabbitMqBindingMode BindingMode { get; private set; }

    /// <summary>
    /// Sets the binding mode for the binding.
    /// </summary>
    /// <param name="bindingMode">The binding mode to use.</param>
    /// <returns>The same builder for chaining.</returns>
    public RabbitMqQueueBindingBuilder WithBindingMode(RabbitMqBindingMode bindingMode)
    {
        BindingMode = bindingMode;
        return this;
    }

    /// <summary>
    /// Adds a binding argument (for example a header match for a headers exchange).
    /// </summary>
    /// <param name="name">The argument name.</param>
    /// <param name="value">The argument value.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="name" /> is null or whitespace.</exception>
    public RabbitMqQueueBindingBuilder WithArgument(string name, object? value)
    {
        _arguments[RequireText(name, nameof(name))] = value;
        return this;
    }

    /// <summary>
    /// Builds the immutable <see cref="RabbitMqQueueBindingDefinition" /> from the configured values.
    /// </summary>
    /// <returns>The queue binding definition.</returns>
    public RabbitMqQueueBindingDefinition Build()
    {
        return new RabbitMqQueueBindingDefinition(
            SourceExchangeName,
            QueueName,
            RoutingKey,
            BindingMode,
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
