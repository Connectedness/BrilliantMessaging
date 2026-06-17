using System.Collections.Generic;

namespace Bmf.Transport.RabbitMq;

/// <summary>
/// An immutable exchange declaration produced by <see cref="RabbitMqExchangeBuilder" />.
/// </summary>
/// <param name="Name">The exchange name.</param>
/// <param name="Type">The exchange type (for example <c>direct</c>, <c>topic</c>, <c>fanout</c>, or <c>headers</c>).</param>
/// <param name="DeclareMode">Whether and how the exchange is declared at provisioning time.</param>
/// <param name="Durable">Whether the exchange survives a broker restart.</param>
/// <param name="AutoDelete">Whether the exchange is deleted once the last queue is unbound.</param>
/// <param name="Arguments">The exchange declaration arguments.</param>
public sealed record RabbitMqExchangeDefinition(
    string Name,
    string Type,
    RabbitMqDeclareMode DeclareMode,
    bool Durable,
    bool AutoDelete,
    IReadOnlyDictionary<string, object?> Arguments
);
