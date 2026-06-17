using System.Threading.Tasks;

namespace Bmf.Core.Messaging.Inbound;

/// <summary>
/// Represents a single stage of the inbound message pipeline: it processes an
/// <see cref="IncomingMessageContext" /> and returns a task that completes when the stage is done. Middleware is
/// composed into a chain of these delegates by <see cref="MessagePipelineBuilder" />.
/// </summary>
/// <param name="context">The context for the message being processed.</param>
/// <returns>A task that completes when the stage has finished processing the message.</returns>
public delegate Task MessageDelegate(IncomingMessageContext context);
