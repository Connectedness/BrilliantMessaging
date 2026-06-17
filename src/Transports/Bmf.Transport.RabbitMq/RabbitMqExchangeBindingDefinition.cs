using System.Collections.Generic;

namespace Bmf.Transport.RabbitMq;

/// <summary>
/// An immutable binding from a source exchange to a destination exchange, produced by
/// <see cref="RabbitMqExchangeBindingBuilder" />.
/// </summary>
/// <param name="SourceExchangeName">The name of the source exchange.</param>
/// <param name="DestinationExchangeName">The name of the destination exchange.</param>
/// <param name="RoutingKey">The binding routing key.</param>
/// <param name="BindingMode">Whether the binding is declared at provisioning time.</param>
/// <param name="Arguments">The binding arguments.</param>
public sealed record RabbitMqExchangeBindingDefinition(
    string SourceExchangeName,
    string DestinationExchangeName,
    string RoutingKey,
    RabbitMqBindingMode BindingMode,
    IReadOnlyDictionary<string, object?> Arguments
) : RabbitMqBindingDefinition(SourceExchangeName, RoutingKey, BindingMode, Arguments);
