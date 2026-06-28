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
/// Choose <see cref="Delete" /> to unbind a queue binding at provisioning time. The provisioner calls
/// <c>QueueUnbindAsync</c> with the binding's recorded arguments so a headers-exchange binding is matched
/// correctly. A broker not-found error for an already-absent binding is treated as success, making
/// <see cref="Delete" /> idempotent across restarts. Use <see cref="Delete" /> as the Update-1 unbind that stops
/// new messages flowing to an old queue: introduce a replacement queue under a new name, add the new binding as
/// <see cref="Active" />, and flip the old binding to <see cref="Delete" />. The provisioner runs all
/// <see cref="Active" /> bindings before any <see cref="Delete" /> bindings, so routing continuity is preserved.
/// <see cref="Delete" /> is defined on this shared enum so it reads correctly for exchange bindings too, but
/// exchange-binding deletion is out of scope and surfaces as an <see cref="System.ArgumentOutOfRangeException" />
/// at provisioning time.
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
    /// Remove the binding at provisioning time. For queue bindings, the provisioner unbinds the queue from the
    /// source exchange. A not-found error for an already-absent binding is treated as success. Exchange-binding
    /// deletion is out of scope and throws at provisioning time.
    /// </summary>
    Delete = 2
}
