namespace BrilliantMessaging.Transport.RabbitMq;

/// <summary>
/// Controls whether and how an exchange or queue is declared when a topology is provisioned.
/// </summary>
/// <remarks>
/// <para>
/// Choose <see cref="Active" /> (the default) to declare the resource, creating it if absent. Choose
/// <see cref="Passive" /> to only assert the resource exists with the expected settings, failing provisioning
/// when it does not — useful when an operator owns the resource. Choose <see cref="Skip" /> to leave the
/// resource entirely to external management.
/// </para>
/// <para>
/// Choose <see cref="Delete" /> to remove a queue at provisioning time. The provisioner first reads the queue's
/// ready-message count with a passive declare, refuses with a clear "drain first" error when the queue is not
/// empty, and only then deletes it. The delete is intentionally independent of attached consumers: in an
/// init-container / rolling deployment the previous version's consumers are still connected to the drained queue
/// when the replacement is provisioned, so requiring zero consumers would block the deployment. Those consumers
/// receive a consumer-cancel as the queue is removed, which is expected since they are about to be replaced.
/// A <c>404 NOT_FOUND</c> for an already-absent queue is treated as success (the desired state — resource absent
/// — is already achieved), making <see cref="Delete" /> idempotent across restarts. Use <see cref="Delete" /> as
/// the last step of the introduce → drain → delete workflow: deploy a replacement queue under a new name, let the
/// old queue fully drain, then flip the old queue's declare mode to <see cref="Delete" />.
/// </para>
/// <para>
/// The empty check counts only ready messages, not messages a still-attached consumer holds unacknowledged, so
/// ensure the old queue has fully drained before flipping it to <see cref="Delete" /> — an in-flight unacknowledged
/// message is discarded together with the queue. <see cref="Delete" /> is defined on this shared enum so it reads
/// correctly for exchanges too, but exchange deletion is out of scope and surfaces as an
/// <see cref="System.ArgumentOutOfRangeException" /> at provisioning time.
/// </para>
/// </remarks>
public enum RabbitMqDeclareMode
{
    /// <summary>
    /// Do not touch the resource; assume it is managed externally.
    /// </summary>
    Skip = 0,

    /// <summary>
    /// Passively assert the resource exists with the expected settings, failing if it does not.
    /// </summary>
    Passive = 1,

    /// <summary>
    /// Actively declare the resource, creating it if it does not already exist.
    /// </summary>
    Active = 2,

    /// <summary>
    /// Remove the resource at provisioning time. For queues, the provisioner deletes the queue only when it has
    /// no ready messages, failing safely with a clear "drain first" error when the queue is not yet empty; the
    /// delete proceeds regardless of attached consumers. A <c>404 NOT_FOUND</c> is treated as success. Exchange
    /// deletion is out of scope and throws at provisioning time.
    /// </summary>
    Delete = 3
}
