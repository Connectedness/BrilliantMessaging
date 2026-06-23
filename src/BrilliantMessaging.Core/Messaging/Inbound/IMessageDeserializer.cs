using System.Threading;
using System.Threading.Tasks;

namespace BrilliantMessaging.Core.Messaging.Inbound;

/// <summary>
/// The inbound counterpart to <see cref="Outbound.IMessageSerializer" />: turns a delivered
/// <see cref="TransportMessage" /> into a message object. Unlike serialization, this seam is
/// CloudEvents-agnostic — an implementation reads the neutral <see cref="IncomingMessageContext" />
/// (its <see cref="IncomingMessageContext.Transport" />, <see cref="IncomingMessageContext.Services" />,
/// and any <see cref="IncomingMessageContext.Items" /> contributed by the inspector) and never a
/// CloudEvents type. Configure a custom implementation per endpoint through the handler configuration passed to
/// <c>Handle</c> or <c>HandleNamed</c>; the default decodes <see cref="TransportMessage.Body" /> through the configured
/// payload codec.
/// </summary>
public interface IMessageDeserializer
{
    /// <summary>
    /// Deserializes the delivered message into <see cref="IncomingMessageContext.MessageType" />.
    /// </summary>
    /// <param name="context">
    /// The neutral context for the delivery. The implementation is read-only over it: it returns the
    /// deserialized object and must not assign <see cref="IncomingMessageContext.Message" /> or settle
    /// <see cref="IncomingMessageContext.Acknowledgement" /> — the deserialization middleware owns both.
    /// </param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The deserialized message, or <see langword="null" />.</returns>
    ValueTask<object?> DeserializeAsync(
        IncomingMessageContext context,
        CancellationToken cancellationToken = default
    );
}
