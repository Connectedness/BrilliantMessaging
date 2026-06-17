namespace Bmf.Transport.RabbitMq;

/// <summary>
/// Controls whether and how an exchange or queue is declared when a topology is provisioned.
/// </summary>
/// <remarks>
/// Choose <see cref="Active" /> (the default) to declare the resource, creating it if absent. Choose
/// <see cref="Passive" /> to only assert the resource exists with the expected settings, failing provisioning
/// when it does not — useful when an operator owns the resource. Choose <see cref="Skip" /> to leave the
/// resource entirely to external management.
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
    Active = 2
}
