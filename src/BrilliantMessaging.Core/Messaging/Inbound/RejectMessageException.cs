using System;

namespace BrilliantMessaging.Core.Messaging.Inbound;

/// <summary>
/// Signals that the current inbound message should be treated as poison and rejected without broker redelivery.
/// </summary>
public sealed class RejectMessageException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="RejectMessageException" /> class.
    /// </summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The exception that caused this rejection.</param>
    public RejectMessageException(
        string message = "The inbound message was explicitly rejected.",
        Exception? innerException = null
    )
        : base(message, innerException) { }
}
