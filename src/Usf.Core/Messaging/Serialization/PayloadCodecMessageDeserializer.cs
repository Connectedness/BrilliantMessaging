using System;
using System.Threading;
using System.Threading.Tasks;
using Usf.Core.Messaging.Errors;

namespace Usf.Core.Messaging.Serialization;

/// <summary>
/// The default <see cref="IMessageDeserializer" />. It decodes <see cref="TransportMessage.Body" />
/// through the configured <see cref="IPayloadCodec" /> into the context's
/// <see cref="IncomingMessageContext.MessageType" />. Because USF emits and reads CloudEvents in
/// binary content mode (the event data is the transport body), this is correct for both CloudEvents
/// and raw deliveries, so the default inbound path needs no CloudEvents type. Endpoints that only
/// switch wire codecs swap <see cref="IPayloadCodec" /> and keep this deserializer; genuinely
/// different framing warrants a custom <see cref="IMessageDeserializer" />.
/// </summary>
public sealed class PayloadCodecMessageDeserializer : IMessageDeserializer
{
    private readonly IPayloadCodec _payloadCodec;

    /// <summary>
    /// Initializes a new instance of the <see cref="PayloadCodecMessageDeserializer" /> class.
    /// </summary>
    /// <param name="payloadCodec">The codec used to decode the transport body.</param>
    /// <exception cref="ArgumentNullException"><paramref name="payloadCodec" /> is <see langword="null" />.</exception>
    public PayloadCodecMessageDeserializer(IPayloadCodec payloadCodec)
    {
        _payloadCodec = payloadCodec ?? throw new ArgumentNullException(nameof(payloadCodec));
    }

    /// <inheritdoc />
    public ValueTask<object?> DeserializeAsync(
        IncomingMessageContext context,
        CancellationToken cancellationToken = default
    )
    {
        if (context is null)
        {
            throw new ArgumentNullException(nameof(context));
        }

        try
        {
            return new ValueTask<object?>(_payloadCodec.Decode(context.Transport.Body, context.MessageType));
        }
        catch (Exception exception)
        {
            throw new MessageDeserializationException(context.MessageType, exception);
        }
    }
}
