using System;

namespace Usf.Core.Messaging.Errors;

public sealed class OutboundTargetNotRoutableException : Exception
{
    public OutboundTargetNotRoutableException(string targetName, Type messageType)
        : base(
            $"Outbound target '{targetName}' does not support routing-key publishing for messages of type '{messageType}'."
        )
    {
        TargetName = targetName;
        MessageType = messageType;
    }

    public string TargetName { get; }

    public Type MessageType { get; }
}
