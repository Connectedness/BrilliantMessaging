namespace BrilliantMessaging.Transport.RabbitMq;

/// <summary>
/// Controls whether a binding is declared when a topology is provisioned.
/// </summary>
/// <remarks>
/// <para>
/// Choose <see cref="Active" /> (the default) to declare the binding. Choose <see cref="Skip" /> to leave the
/// binding to external management.
/// </para>
/// <para>
/// Choose <see cref="Delete" /> to unbind a binding at provisioning time. The provisioner calls
/// <c>QueueUnbindAsync</c> or <c>ExchangeUnbindAsync</c> with the binding's recorded arguments so a
/// headers-exchange binding is matched correctly. A broker not-found error for an already-absent binding is
/// treated as success, making <see cref="Delete" /> idempotent across restarts. Use <see cref="Delete" /> as the
/// Update-1 unbind that stops new messages flowing to an old queue or exchange: introduce a replacement resource
/// under a new name, add the new binding as <see cref="Active" />, and flip the old binding to
/// <see cref="Delete" />. The provisioner runs all <see cref="Active" /> bindings before any <see cref="Delete" />
/// bindings, so routing continuity is preserved.
/// </para>
/// </remarks>
public enum RabbitMqBindingMode
{
    /// <summary>
    /// Do not declare the binding; assume it is managed externally.
    /// </summary>
    Skip = 0,

    /// <summary>
    /// Actively declare the binding.
    /// </summary>
    Active = 1,

    /// <summary>
    /// Remove the binding at provisioning time. The provisioner unbinds the queue or destination exchange from
    /// the source exchange, using the binding's recorded arguments so a headers-exchange binding is matched.
    /// A not-found error for an already-absent binding is treated as success.
    /// </summary>
    Delete = 2
}
