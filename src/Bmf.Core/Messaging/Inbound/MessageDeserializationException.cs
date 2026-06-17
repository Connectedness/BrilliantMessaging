using System;

namespace Bmf.Core.Messaging.Inbound;

/// <summary>
/// Thrown when decoding an inbound message body into its resolved message type fails. The underlying error is
/// preserved as the inner exception.
/// </summary>
public sealed class MessageDeserializationException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="MessageDeserializationException" /> class.
    /// </summary>
    /// <param name="messageType">The message type that failed to deserialize.</param>
    /// <param name="innerException">The underlying deserialization error.</param>
    public MessageDeserializationException(Type messageType, Exception innerException)
        : base($"Deserialization failed for message type '{messageType}'.", innerException)
    {
        MessageType = messageType;
    }

    /// <summary>
    /// Gets the message type that failed to deserialize.
    /// </summary>
    public Type MessageType { get; }
}
