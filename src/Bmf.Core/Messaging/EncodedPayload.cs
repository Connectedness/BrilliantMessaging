namespace Bmf.Core.Messaging;

/// <summary>
/// The output of an <see cref="IPayloadCodec" />: the encoded message bytes together with the content type that
/// describes how they were encoded.
/// </summary>
/// <param name="Data">The encoded payload bytes.</param>
/// <param name="DataContentType">The content type of the encoded payload (for example <c>application/json</c>).</param>
public readonly record struct EncodedPayload(byte[] Data, string DataContentType);
