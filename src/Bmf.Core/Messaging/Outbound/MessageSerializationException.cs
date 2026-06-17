using System;

namespace Bmf.Core.Messaging.Outbound;

/// <summary>
/// Thrown when serializing an outbound message fails. The underlying error is preserved as the inner exception.
/// </summary>
public sealed class MessageSerializationException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="MessageSerializationException" /> class.
    /// </summary>
    /// <param name="messageType">The message type that failed to serialize.</param>
    /// <param name="innerException">The underlying serialization error.</param>
    public MessageSerializationException(Type messageType, Exception innerException)
        : base($"Serialization failed for message type '{messageType}'.", innerException)
    {
        MessageType = messageType;
    }

    /// <summary>
    /// Gets the message type that failed to serialize.
    /// </summary>
    public Type MessageType { get; }
}
