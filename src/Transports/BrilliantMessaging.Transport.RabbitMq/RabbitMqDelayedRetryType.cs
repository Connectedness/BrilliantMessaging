namespace BrilliantMessaging.Transport.RabbitMq;

/// <summary>
/// Configures the delayed-retry behavior of a quorum queue (<c>x-delayed-retry-type</c>), controlling which
/// redelivered messages are held back for the configured delay before being retried.
/// </summary>
/// <remarks>
/// Requires RabbitMQ 4.3 or later and a quorum queue. The delay range is set with
/// <see cref="RabbitMqQueueBuilder.WithDelayedRetry(RabbitMqDelayedRetryType, System.TimeSpan, System.TimeSpan?)" />.
/// </remarks>
public enum RabbitMqDelayedRetryType
{
    /// <summary>
    /// Delayed retry is disabled; redelivered messages are dispatched immediately.
    /// </summary>
    Disabled = 0,

    /// <summary>
    /// All redelivered messages are delayed before retry.
    /// </summary>
    All,

    /// <summary>
    /// Only messages whose previous delivery was a consumer failure are delayed before retry.
    /// </summary>
    Failed,

    /// <summary>
    /// Only messages that were returned to the queue without a consumer delivery are delayed before retry.
    /// </summary>
    Returned
}
