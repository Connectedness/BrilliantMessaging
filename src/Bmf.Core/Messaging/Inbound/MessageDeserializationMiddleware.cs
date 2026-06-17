using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;

namespace Bmf.Core.Messaging.Inbound;

/// <summary>
/// Inbound middleware that decodes the transport body into the resolved message type (using the endpoint's
/// deserializer) and stores it on the context before invoking the next stage. It is a no-op when the message has
/// already been materialized by an inspector.
/// </summary>
public sealed class MessageDeserializationMiddleware : IMessageMiddleware
{
    /// <inheritdoc />
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="context" /> or <paramref name="next" /> is <see langword="null" />.</exception>
    public async Task InvokeAsync(IncomingMessageContext context, MessageDelegate next)
    {
        if (context is null)
        {
            throw new ArgumentNullException(nameof(context));
        }

        if (next is null)
        {
            throw new ArgumentNullException(nameof(next));
        }

        if (context.Message is null)
        {
            var deserializer = (IMessageDeserializer) context.Services.GetRequiredService(
                context.Endpoint.DeserializerType
            );
            context.Message = await deserializer
               .DeserializeAsync(context, context.CancellationToken)
               .ConfigureAwait(false);
        }

        await next(context).ConfigureAwait(false);
    }
}
