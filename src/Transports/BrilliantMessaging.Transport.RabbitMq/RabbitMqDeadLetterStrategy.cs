namespace BrilliantMessaging.Transport.RabbitMq;

/// <summary>
/// Configures the dead-lettering strategy of a quorum queue (<c>x-dead-letter-strategy</c>), controlling whether
/// dead-lettered messages can be delivered more than once to the dead-letter exchange.
/// </summary>
/// <remarks>
/// Requires RabbitMQ 4.3 or later and a quorum queue. The <see cref="AtLeastOnce" /> strategy additionally
/// requires <c>x-overflow = reject-publish</c> at runtime; this constraint is enforced by the broker, not by the
/// compiler.
/// </remarks>
public enum RabbitMqDeadLetterStrategy
{
    /// <summary>
    /// A dead-lettered message is delivered to the dead-letter exchange at most once (best effort).
    /// </summary>
    AtMostOnce = 0,

    /// <summary>
    /// A dead-lettered message is guaranteed to be delivered to the dead-letter exchange at least once.
    /// Requires <c>x-overflow = reject-publish</c> on the queue.
    /// </summary>
    AtLeastOnce
}
