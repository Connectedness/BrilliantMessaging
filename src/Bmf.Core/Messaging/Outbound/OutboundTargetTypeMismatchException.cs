using System;

namespace Bmf.Core.Messaging.Outbound;

/// <summary>
/// Thrown when a named outbound target exists but is typed for a different message type than the one requested.
/// </summary>
public sealed class OutboundTargetTypeMismatchException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="OutboundTargetTypeMismatchException" /> class.
    /// </summary>
    /// <param name="targetName">The name of the target.</param>
    /// <param name="actualMessageType">The message type that was requested.</param>
    /// <param name="expectedMessageType">The message type the target is actually typed for, or <see langword="null" /> when the target is not typed.</param>
    public OutboundTargetTypeMismatchException(string targetName, Type actualMessageType, Type? expectedMessageType)
        : base(
            $"Outbound target '{targetName}' cannot publish messages of type '{actualMessageType}'. Expected '{GetExpectedMessageTypeName(expectedMessageType)}'."
        )
    {
        TargetName = targetName;
        ActualMessageType = actualMessageType;
        ExpectedMessageType = expectedMessageType;
    }

    /// <summary>
    /// Gets the name of the target.
    /// </summary>
    public string TargetName { get; }

    /// <summary>
    /// Gets the message type that was requested.
    /// </summary>
    public Type ActualMessageType { get; }

    /// <summary>
    /// Gets the message type the target is typed for, or <see langword="null" /> when the target is not typed.
    /// </summary>
    public Type? ExpectedMessageType { get; }

    private static string GetExpectedMessageTypeName(Type? expectedMessageType)
    {
        return expectedMessageType?.ToString() ?? "no typed message";
    }
}
