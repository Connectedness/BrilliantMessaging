namespace Bmf.Transport.RabbitMq;

/// <summary>
/// Controls whether a binding is declared when a topology is provisioned.
/// </summary>
/// <remarks>
/// Choose <see cref="Active" /> (the default) to declare the binding. Choose <see cref="Skip" /> to leave the
/// binding to external management.
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
    Active = 1
}
