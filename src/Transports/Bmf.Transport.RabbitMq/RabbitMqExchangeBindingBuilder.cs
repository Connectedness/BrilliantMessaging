using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace Bmf.Transport.RabbitMq;

/// <summary>
/// Fluent builder for a binding from a source exchange to a destination exchange, collecting the routing key,
/// binding mode, and binding arguments into a <see cref="RabbitMqExchangeBindingDefinition" />.
/// </summary>
public sealed class RabbitMqExchangeBindingBuilder
{
    private readonly Dictionary<string, object?> _arguments = new (StringComparer.Ordinal);

    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMqExchangeBindingBuilder" /> class.
    /// </summary>
    /// <param name="sourceExchangeName">The name of the source exchange.</param>
    /// <param name="destinationExchangeName">The name of the destination exchange.</param>
    /// <param name="routingKey">The binding routing key; an empty string is permitted.</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="sourceExchangeName" /> or <paramref name="destinationExchangeName" /> is null or whitespace.</exception>
    public RabbitMqExchangeBindingBuilder(
        string sourceExchangeName,
        string destinationExchangeName,
        string routingKey
    )
    {
        SourceExchangeName = RequireText(sourceExchangeName, nameof(sourceExchangeName));
        DestinationExchangeName = RequireText(destinationExchangeName, nameof(destinationExchangeName));
        RoutingKey = routingKey ?? string.Empty;
        BindingMode = RabbitMqBindingMode.Active;
    }

    /// <summary>
    /// Gets the name of the source exchange.
    /// </summary>
    public string SourceExchangeName { get; }

    /// <summary>
    /// Gets the name of the destination exchange.
    /// </summary>
    public string DestinationExchangeName { get; }

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
    public RabbitMqExchangeBindingBuilder WithBindingMode(RabbitMqBindingMode bindingMode)
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
    public RabbitMqExchangeBindingBuilder WithArgument(string name, object? value)
    {
        _arguments[RequireText(name, nameof(name))] = value;
        return this;
    }

    /// <summary>
    /// Builds the immutable <see cref="RabbitMqExchangeBindingDefinition" /> from the configured values.
    /// </summary>
    /// <returns>The exchange binding definition.</returns>
    public RabbitMqExchangeBindingDefinition Build()
    {
        return new RabbitMqExchangeBindingDefinition(
            SourceExchangeName,
            DestinationExchangeName,
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
