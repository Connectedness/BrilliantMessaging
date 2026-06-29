using System.Collections.Immutable;

namespace BrilliantMessaging.Transport.InMemory.Inbound;

/// <summary>
/// An immutable declaration of an in-memory consumer on a topic, produced by
/// <see cref="InMemoryInboundConsumerBuilder" />.
/// </summary>
/// <param name="Topic">The declared topic the consumer subscribes to.</param>
/// <param name="Concurrency">The number of background workers processing the consumer's deliveries.</param>
/// <param name="Handlers">The handler registrations for the consumer.</param>
/// <param name="DeliveryPolicy">The failure-handling policy for the consumer.</param>
public sealed record InMemoryInboundConsumerDefinition(
    string Topic,
    int Concurrency,
    ImmutableArray<InMemoryInboundHandlerDefinition> Handlers,
    InMemoryDeliveryPolicy DeliveryPolicy
);
