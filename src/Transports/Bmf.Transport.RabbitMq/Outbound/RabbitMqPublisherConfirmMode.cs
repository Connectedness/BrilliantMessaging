namespace Bmf.Transport.RabbitMq.Outbound;

/// <summary>
/// Configures RabbitMQ publisher confirmations for every channel in a channel group.
/// </summary>
/// <remarks>
/// Confirmation tracking serializes outstanding publishes per channel while each publish awaits a broker
/// outcome. A single-channel group therefore preserves per-target ordering at the cost of one broker
/// round-trip per publish. Increase the group's maximum channel count for more throughput when relaxed
/// ordering is acceptable, or explicitly select <see cref="FireAndForget" /> when message loss is acceptable.
/// </remarks>
public enum RabbitMqPublisherConfirmMode
{
    /// <summary>
    /// Publishes without waiting for a broker acknowledgement. Fastest, but a broker nack or an unroutable
    /// message can be lost silently.
    /// </summary>
    FireAndForget = 0,

    /// <summary>
    /// Waits for a broker confirmation for each publish, surfacing nacks and unroutable returns as a
    /// <see cref="Bmf.Core.Messaging.Outbound.MessageDeliveryException" />. Required for mandatory routing.
    /// </summary>
    Confirms = 1
}
