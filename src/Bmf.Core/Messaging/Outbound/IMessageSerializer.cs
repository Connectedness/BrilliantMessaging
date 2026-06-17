using System.Threading;
using System.Threading.Tasks;

namespace Bmf.Core.Messaging.Outbound;

/// <summary>
/// Serializes outbound messages into <see cref="CloudEventEnvelope" /> instances. This is the outbound
/// serialization extension point; the default implementation combines an <see cref="IPayloadCodec" /> with the
/// CloudEvents metadata, but a custom serializer can replace the whole strategy.
/// </summary>
public interface IMessageSerializer
{
    /// <summary>
    /// Serializes a message into a CloudEvents envelope.
    /// </summary>
    /// <typeparam name="T">The message type to serialize.</typeparam>
    /// <param name="message">The message to serialize.</param>
    /// <param name="metadata">The call-site-owned CloudEvents attributes.</param>
    /// <param name="type">
    /// The already-resolved CloudEvents <c>type</c> discriminator.
    /// </param>
    /// <param name="dataSchema">The already-resolved CloudEvents <c>dataschema</c> value, if any.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The serialized CloudEvents envelope.</returns>
    ValueTask<CloudEventEnvelope> SerializeAsync<T>(
        T message,
        in CloudEventMetadata metadata,
        string? type,
        string? dataSchema,
        CancellationToken cancellationToken = default
    );
}
