using System;

namespace Bmf.Core.Messaging.Outbound;

/// <summary>
/// Thrown when a routing-key publish is requested against an outbound target that does not support per-publish
/// routing keys.
/// </summary>
public sealed class OutboundTargetNotRoutableException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="OutboundTargetNotRoutableException" /> class.
    /// </summary>
    /// <param name="targetName">The name of the non-routable target.</param>
    /// <param name="messageType">The message type that was being published.</param>
    public OutboundTargetNotRoutableException(string targetName, Type messageType)
        : base(
            $"Outbound target '{targetName}' does not support routing-key publishing for messages of type '{messageType}'."
        )
    {
        TargetName = targetName;
        MessageType = messageType;
    }

    /// <summary>
    /// Gets the name of the non-routable target.
    /// </summary>
    public string TargetName { get; }

    /// <summary>
    /// Gets the message type that was being published.
    /// </summary>
    public Type MessageType { get; }
}
