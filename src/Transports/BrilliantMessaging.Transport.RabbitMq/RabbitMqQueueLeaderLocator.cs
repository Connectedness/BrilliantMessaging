namespace BrilliantMessaging.Transport.RabbitMq;

/// <summary>
/// Configures the queue leader locator (<c>x-queue-leader-locator</c>) of a quorum queue, controlling how the
/// leader for a new quorum queue is selected across the cluster.
/// </summary>
/// <remarks>
/// This argument is supported by quorum queues only; the compiler rejects it on classic or unknown queue types.
/// </remarks>
public enum RabbitMqQueueLeaderLocator
{
    /// <summary>
    /// The client that declares the queue hosts the leader replica on its connected node.
    /// </summary>
    ClientLocal = 0,

    /// <summary>
    /// The leader replica is placed on the node with the fewest running quorum queue leaders.
    /// </summary>
    Balanced
}
