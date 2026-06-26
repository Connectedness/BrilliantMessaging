namespace BrilliantMessaging.Transport.RabbitMq;

/// <summary>
/// Identifies the RabbitMQ queue type relevant to inbound redelivery behavior.
/// </summary>
public enum RabbitMqQueueType
{
    /// <summary>
    /// The queue type is not known to the topology compiler.
    /// </summary>
    Unknown = 0,

    /// <summary>
    /// A classic RabbitMQ queue.
    /// </summary>
    Classic,

    /// <summary>
    /// A quorum RabbitMQ queue.
    /// </summary>
    Quorum
}
