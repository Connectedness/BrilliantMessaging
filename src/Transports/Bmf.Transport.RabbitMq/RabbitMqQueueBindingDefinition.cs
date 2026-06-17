using System.Collections.Generic;

namespace Bmf.Transport.RabbitMq;

/// <summary>
/// An immutable binding from a source exchange to a queue, produced by <see cref="RabbitMqQueueBindingBuilder" />.
/// </summary>
/// <param name="SourceExchangeName">The name of the source exchange.</param>
/// <param name="QueueName">The name of the bound queue.</param>
/// <param name="RoutingKey">The binding routing key.</param>
/// <param name="BindingMode">Whether the binding is declared at provisioning time.</param>
/// <param name="Arguments">The binding arguments.</param>
public sealed record RabbitMqQueueBindingDefinition(
    string SourceExchangeName,
    string QueueName,
    string RoutingKey,
    RabbitMqBindingMode BindingMode,
    IReadOnlyDictionary<string, object?> Arguments
) : RabbitMqBindingDefinition(SourceExchangeName, RoutingKey, BindingMode, Arguments);
