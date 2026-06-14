using System;
using System.Threading;
using System.Threading.Tasks;
using Usf.Core.Messaging.Errors;

namespace Usf.Core.Messaging.Serialization;

public sealed class PayloadCodecMessageDeserializer : IMessageDeserializer
{
    private readonly IPayloadCodec _payloadCodec;

    public PayloadCodecMessageDeserializer(IPayloadCodec payloadCodec)
    {
        _payloadCodec = payloadCodec ?? throw new ArgumentNullException(nameof(payloadCodec));
    }

    public ValueTask<object?> DeserializeAsync(
        IncomingMessageContext context,
        Type messageType,
        CancellationToken cancellationToken = default
    )
    {
        if (context is null)
        {
            throw new ArgumentNullException(nameof(context));
        }

        if (messageType is null)
        {
            throw new ArgumentNullException(nameof(messageType));
        }

        try
        {
            return new ValueTask<object?>(_payloadCodec.Decode(context.Transport.Body, messageType));
        }
        catch (Exception exception)
        {
            throw new MessageDeserializationException(messageType, exception);
        }
    }
}
