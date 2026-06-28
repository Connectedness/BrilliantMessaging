namespace BrilliantMessaging.Transport.RabbitMq;

/// <summary>
/// Configures the overflow behavior of a queue (<c>x-overflow</c>), controlling what happens when the maximum
/// length or length-bytes limit is reached.
/// </summary>
public enum RabbitMqOverflow
{
    /// <summary>
    /// Messages are dropped from the head of the queue (the oldest messages) when the maximum length is reached.
    /// This is the default RabbitMQ behavior.
    /// </summary>
    DropHead = 0,

    /// <summary>
    /// Publishes are rejected when the maximum length is reached; the broker NACKs the publish so the publisher
    /// can react.
    /// </summary>
    RejectPublish,

    /// <summary>
    /// Publishes are rejected and the rejected message is dead-lettered when the maximum length is reached.
    /// This value is supported by classic queues only; quorum queues do not support <c>reject-publish-dlx</c>.
    /// </summary>
    RejectPublishDlx
}
