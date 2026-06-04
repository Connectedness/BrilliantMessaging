namespace Usf.Core.Messaging;

/// <summary>
/// Configures application-level CloudEvents attributes.
/// </summary>
public sealed class CloudEventsOptions
{
    /// <summary>
    /// Gets or sets the non-empty URI-reference identifying the application that emits events.
    /// </summary>
    public string? Source { get; set; }
}
