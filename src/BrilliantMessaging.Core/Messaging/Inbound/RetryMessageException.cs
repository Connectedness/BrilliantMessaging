using System;

namespace BrilliantMessaging.Core.Messaging.Inbound;

/// <summary>
/// Signals that the current inbound message should be treated as retryable when the endpoint's redelivery
/// classifier permits broker redelivery.
/// </summary>
public sealed class RetryMessageException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="RetryMessageException" /> class.
    /// </summary>
    public RetryMessageException()
        : base("The inbound message was explicitly marked retryable.") { }

    /// <summary>
    /// Initializes a new instance of the <see cref="RetryMessageException" /> class with a custom message.
    /// </summary>
    /// <param name="message">The exception message.</param>
    public RetryMessageException(string message)
        : base(message) { }

    /// <summary>
    /// Initializes a new instance of the <see cref="RetryMessageException" /> class with a custom message and
    /// inner exception.
    /// </summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The exception that caused this retry marker.</param>
    public RetryMessageException(string message, Exception innerException)
        : base(message, innerException) { }
}
