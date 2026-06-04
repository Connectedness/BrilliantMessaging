namespace Usf.Core.Messaging.Serialization;

public readonly record struct EncodedPayload(byte[] Data, string DataContentType);
