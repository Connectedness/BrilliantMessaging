using System.Collections.Generic;

namespace Bmf.Transport.RabbitMq;

/// <summary>
/// The immutable base for a RabbitMQ binding declaration. Concrete bindings are
/// <see cref="RabbitMqQueueBindingDefinition" /> (exchange-to-queue) and
/// <see cref="RabbitMqExchangeBindingDefinition" /> (exchange-to-exchange).
/// </summary>
/// <param name="SourceExchangeName">The name of the source exchange.</param>
/// <param name="RoutingKey">The binding routing key.</param>
/// <param name="BindingMode">Whether the binding is declared at provisioning time.</param>
/// <param name="Arguments">The binding arguments.</param>
public abstract record RabbitMqBindingDefinition(
    string SourceExchangeName,
    string RoutingKey,
    RabbitMqBindingMode BindingMode,
    IReadOnlyDictionary<string, object?> Arguments
);
