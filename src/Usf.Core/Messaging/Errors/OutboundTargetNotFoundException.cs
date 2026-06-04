using System;

namespace Usf.Core.Messaging.Errors;

public sealed class OutboundTargetNotFoundException : Exception
{
    public OutboundTargetNotFoundException(Type messageType)
        : base($"No outbound target is registered for message type '{messageType}'.")
    {
        MessageType = messageType;
    }

    public OutboundTargetNotFoundException(string targetName)
        : base($"No outbound target is registered with name '{targetName}'.")
    {
        TargetName = targetName;
    }

    public Type? MessageType { get; }

    public string? TargetName { get; }
}
