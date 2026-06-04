namespace Usf.Core.Messaging.Serialization;

/// <summary>
/// Encodes the data section of a CloudEvent.
/// </summary>
public interface IPayloadCodec
{
    EncodedPayload Encode<T>(T message);
}
