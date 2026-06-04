using System.Collections.Generic;

namespace Usf.Transport.RabbitMq.Configuration;

public abstract record RabbitMqBindingDefinition(
    string SourceExchangeName,
    string RoutingKey,
    RabbitMqBindingMode BindingMode,
    IReadOnlyDictionary<string, object?> Arguments
);

public sealed record RabbitMqQueueBindingDefinition(
    string SourceExchangeName,
    string QueueName,
    string RoutingKey,
    RabbitMqBindingMode BindingMode,
    IReadOnlyDictionary<string, object?> Arguments
) : RabbitMqBindingDefinition(SourceExchangeName, RoutingKey, BindingMode, Arguments);

public sealed record RabbitMqExchangeBindingDefinition(
    string SourceExchangeName,
    string DestinationExchangeName,
    string RoutingKey,
    RabbitMqBindingMode BindingMode,
    IReadOnlyDictionary<string, object?> Arguments
) : RabbitMqBindingDefinition(SourceExchangeName, RoutingKey, BindingMode, Arguments);
