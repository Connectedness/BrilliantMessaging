namespace Usf.Transport.RabbitMq.Configuration;

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
    FireAndForget = 0,
    Confirms = 1
}
