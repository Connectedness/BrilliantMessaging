using System;

namespace Bmf.Transport.RabbitMq.Outbound;

/// <summary>
/// An immutable declaration of an outbound channel group, produced by the topology builder.
/// </summary>
/// <param name="Name">The group name.</param>
/// <param name="MaximumChannelCount">The maximum number of channels the group may open.</param>
/// <param name="PublisherConfirmMode">The publisher-confirm mode, or <see langword="null" /> to inherit the topology default.</param>
/// <param name="PublisherConfirmTimeout">The confirm timeout, or <see langword="null" /> to inherit the topology default.</param>
public sealed record RabbitMqOutboundChannelGroupDefinition(
    string Name,
    int MaximumChannelCount,
    RabbitMqPublisherConfirmMode? PublisherConfirmMode = null,
    TimeSpan? PublisherConfirmTimeout = null
)
{
    /// <summary>
    /// The reserved name prefix used for channel groups the framework creates implicitly; user-supplied group
    /// names may not start with it.
    /// </summary>
    public const string ReservedImplicitNamePrefix = "$implicit:";
}
