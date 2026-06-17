using System;

namespace Bmf.Core.Messaging.Outbound;

/// <summary>
/// Thrown when no outbound target is registered for a requested message type or target name.
/// </summary>
public sealed class OutboundTargetNotFoundException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="OutboundTargetNotFoundException" /> class for a missing
    /// message-type mapping.
    /// </summary>
    /// <param name="messageType">The message type that has no registered target.</param>
    public OutboundTargetNotFoundException(Type messageType)
        : base($"No outbound target is registered for message type '{messageType}'.")
    {
        MessageType = messageType;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="OutboundTargetNotFoundException" /> class for a missing named
    /// target.
    /// </summary>
    /// <param name="targetName">The target name that was not found.</param>
    public OutboundTargetNotFoundException(string targetName)
        : base($"No outbound target is registered with name '{targetName}'.")
    {
        TargetName = targetName;
    }

    /// <summary>
    /// Gets the message type that has no registered target, or <see langword="null" /> when the lookup was by name.
    /// </summary>
    public Type? MessageType { get; }

    /// <summary>
    /// Gets the target name that was not found, or <see langword="null" /> when the lookup was by message type.
    /// </summary>
    public string? TargetName { get; }
}
