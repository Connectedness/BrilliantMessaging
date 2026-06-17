using System.Threading;
using System.Threading.Tasks;

namespace Bmf.Core.Messaging.Inbound;

/// <summary>
/// Handles inbound messages of type <typeparamref name="TMessage" />. This is the application-level extension
/// point: the framework resolves the handler from the per-message service provider and invokes it at the end of
/// the inbound pipeline.
/// </summary>
/// <typeparam name="TMessage">The message type the handler processes.</typeparam>
public interface IMessageHandler<in TMessage>
{
    /// <summary>
    /// Handles a single inbound message.
    /// </summary>
    /// <param name="message">The deserialized message.</param>
    /// <param name="context">The context for the message, exposing transport details, services, and acknowledgement.</param>
    /// <param name="cancellationToken">A token to observe while handling the message.</param>
    /// <returns>A task that completes when the message has been handled.</returns>
    Task HandleAsync(TMessage message, IncomingMessageContext context, CancellationToken cancellationToken);
}
