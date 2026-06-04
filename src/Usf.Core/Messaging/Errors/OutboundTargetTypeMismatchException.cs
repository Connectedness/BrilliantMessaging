using System;

namespace Usf.Core.Messaging.Errors;

public sealed class OutboundTargetTypeMismatchException : Exception
{
    public OutboundTargetTypeMismatchException(string targetName, Type actualMessageType, Type? expectedMessageType)
        : base(
            $"Outbound target '{targetName}' cannot publish messages of type '{actualMessageType}'. Expected '{GetExpectedMessageTypeName(expectedMessageType)}'."
        )
    {
        TargetName = targetName;
        ActualMessageType = actualMessageType;
        ExpectedMessageType = expectedMessageType;
    }

    public string TargetName { get; }

    public Type ActualMessageType { get; }

    public Type? ExpectedMessageType { get; }

    private static string GetExpectedMessageTypeName(Type? expectedMessageType)
    {
        return expectedMessageType?.ToString() ?? "no typed message";
    }
}
