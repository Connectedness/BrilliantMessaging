using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using Usf.Transport.RabbitMq.Configuration;

namespace Usf.Transport.RabbitMq;

public sealed class RabbitMqQueueBindingBuilder
{
    private readonly Dictionary<string, object?> _arguments = new (StringComparer.Ordinal);

    public RabbitMqQueueBindingBuilder(string exchangeName, string queueName, string routingKey)
    {
        SourceExchangeName = RequireText(exchangeName, nameof(exchangeName));
        QueueName = RequireText(queueName, nameof(queueName));
        RoutingKey = routingKey ?? string.Empty;
        BindingMode = RabbitMqBindingMode.Active;
    }

    public string SourceExchangeName { get; }

    public string QueueName { get; }

    public string RoutingKey { get; }

    public RabbitMqBindingMode BindingMode { get; private set; }

    public RabbitMqQueueBindingBuilder WithBindingMode(RabbitMqBindingMode bindingMode)
    {
        BindingMode = bindingMode;
        return this;
    }

    public RabbitMqQueueBindingBuilder WithArgument(string name, object? value)
    {
        _arguments[RequireText(name, nameof(name))] = value;
        return this;
    }

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
