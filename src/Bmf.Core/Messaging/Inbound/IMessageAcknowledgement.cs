using System.Threading;
using System.Threading.Tasks;

namespace Bmf.Core.Messaging.Inbound;

/// <summary>
/// Acknowledges the outcome of processing an inbound message back to the transport. The handle is exposed on
/// <see cref="IncomingMessageContext" />; under <see cref="MessageAckMode.Manual" /> the application calls it
/// explicitly, while under <see cref="MessageAckMode.Auto" /> the framework calls it on the handler's behalf.
/// </summary>
public interface IMessageAcknowledgement
{
    /// <summary>
    /// Acknowledges successful processing, allowing the transport to remove the message from the queue.
    /// </summary>
    /// <param name="cancellationToken">A token to observe while acknowledging.</param>
    /// <returns>A task that completes when the acknowledgement has been sent.</returns>
    Task AckAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Negatively acknowledges the message, optionally requeuing it for redelivery.
    /// </summary>
    /// <param name="requeue"><see langword="true" /> to requeue the message for another delivery attempt; <see langword="false" /> to discard or dead-letter it.</param>
    /// <param name="cancellationToken">A token to observe while sending the negative acknowledgement.</param>
    /// <returns>A task that completes when the negative acknowledgement has been sent.</returns>
    Task NackAsync(bool requeue, CancellationToken cancellationToken = default);
}
