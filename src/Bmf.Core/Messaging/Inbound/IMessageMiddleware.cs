using System.Threading.Tasks;

namespace Bmf.Core.Messaging.Inbound;

/// <summary>
/// A unit of behaviour in the inbound message pipeline. Implementations inspect or augment the
/// <see cref="IncomingMessageContext" /> and decide whether and when to invoke the next stage, allowing
/// cross-cutting concerns (logging, deserialization, acknowledgement) to wrap the handler.
/// </summary>
public interface IMessageMiddleware
{
    /// <summary>
    /// Processes the incoming message and, when appropriate, invokes <paramref name="next" /> to continue the
    /// pipeline.
    /// </summary>
    /// <param name="context">The context for the message being processed.</param>
    /// <param name="next">The next stage of the pipeline.</param>
    /// <returns>A task that completes when this stage (and any downstream stages it invokes) has finished.</returns>
    Task InvokeAsync(IncomingMessageContext context, MessageDelegate next);
}
