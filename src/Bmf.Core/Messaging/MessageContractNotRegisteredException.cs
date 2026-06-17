using System;

namespace Bmf.Core.Messaging;

/// <summary>
/// Thrown when a message type has no registered canonical CloudEvents discriminator.
/// </summary>
public sealed class MessageContractNotRegisteredException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="MessageContractNotRegisteredException" /> class.
    /// </summary>
    /// <param name="messageType">The unregistered message type.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="messageType" /> is <see langword="null" />.</exception>
    public MessageContractNotRegisteredException(Type messageType)
        : base($"No canonical CloudEvents discriminator is registered for message type '{messageType}'.")
    {
        MessageType = messageType ?? throw new ArgumentNullException(nameof(messageType));
    }

    /// <summary>
    /// Gets the message type that is not registered.
    /// </summary>
    public Type MessageType { get; }
}
