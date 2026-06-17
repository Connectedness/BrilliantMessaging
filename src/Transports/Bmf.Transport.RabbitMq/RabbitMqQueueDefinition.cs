using System.Collections.Generic;

namespace Bmf.Transport.RabbitMq;

/// <summary>
/// An immutable queue declaration produced by <see cref="RabbitMqQueueBuilder" />.
/// </summary>
/// <param name="Name">The queue name.</param>
/// <param name="DeclareMode">Whether and how the queue is declared at provisioning time.</param>
/// <param name="Durable">Whether the queue survives a broker restart.</param>
/// <param name="Exclusive">Whether the queue is exclusive to its declaring connection.</param>
/// <param name="AutoDelete">Whether the queue is deleted once its last consumer disconnects.</param>
/// <param name="Arguments">The queue declaration arguments.</param>
public sealed record RabbitMqQueueDefinition(
    string Name,
    RabbitMqDeclareMode DeclareMode,
    bool Durable,
    bool Exclusive,
    bool AutoDelete,
    IReadOnlyDictionary<string, object?> Arguments
);
