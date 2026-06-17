using System;

namespace Bmf.Core.Messaging;

/// <summary>
/// Encodes and decodes message payloads.
/// </summary>
public interface IPayloadCodec
{
    /// <summary>
    /// Encodes a message into its wire payload, returning the bytes together with their content type.
    /// </summary>
    /// <typeparam name="T">The message type to encode.</typeparam>
    /// <param name="message">The message to encode.</param>
    /// <returns>The encoded payload.</returns>
    EncodedPayload Encode<T>(T message);

    /// <summary>
    /// Decodes a wire payload back into an instance of the given message type.
    /// </summary>
    /// <param name="data">The encoded payload bytes.</param>
    /// <param name="messageType">The target message type to decode into.</param>
    /// <returns>The decoded message, or <see langword="null" /> when the payload represents a null value.</returns>
    object? Decode(ReadOnlyMemory<byte> data, Type messageType);
}
