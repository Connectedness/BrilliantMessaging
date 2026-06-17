namespace Bmf.Transport.RabbitMq.Inbound;

/// <summary>
/// An immutable declaration of an inbound channel group, produced by the topology builder.
/// </summary>
/// <param name="Name">The group name.</param>
/// <param name="MaximumChannelCount">The maximum number of consumer channels the group may open.</param>
/// <param name="PrefetchCount">The per-consumer prefetch (QoS) count.</param>
/// <param name="ConsumerDispatchConcurrency">The consumer dispatch concurrency per channel.</param>
public sealed record RabbitMqInboundChannelGroupDefinition(
    string Name,
    int MaximumChannelCount,
    ushort PrefetchCount,
    ushort ConsumerDispatchConcurrency
)
{
    /// <summary>
    /// The reserved name prefix used for channel groups the framework creates implicitly; user-supplied group
    /// names may not start with it.
    /// </summary>
    public const string ReservedImplicitNamePrefix = "$implicit:";
}
