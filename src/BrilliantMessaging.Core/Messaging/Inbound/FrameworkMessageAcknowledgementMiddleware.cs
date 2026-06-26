using System;
using System.Threading;
using System.Threading.Tasks;

namespace BrilliantMessaging.Core.Messaging.Inbound;

/// <summary>
/// Inbound middleware that implements <see cref="MessageAckMode.Auto" />: it acknowledges the message after the
/// handler succeeds, requeues it on cancellation, and classifies other failures through the endpoint's
/// <see cref="RedeliveryClassifier" />. Under <see cref="MessageAckMode.Manual" /> it leaves acknowledgement
/// entirely to the handler.
/// </summary>
public sealed class FrameworkMessageAcknowledgementMiddleware : IMessageMiddleware
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

        try
        {
            await next(context).ConfigureAwait(false);

            if (context.Endpoint.AckMode == MessageAckMode.Auto)
            {
                await context.Acknowledgement.AckAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (context.CancellationToken.IsCancellationRequested)
        {
            if (context.Endpoint.AckMode == MessageAckMode.Auto)
            {
                await context.Acknowledgement.NackAsync(requeue: true, CancellationToken.None).ConfigureAwait(false);
            }

            throw;
        }
        catch (Exception exception)
        {
            if (context.Endpoint.AckMode == MessageAckMode.Auto)
            {
                var shouldRetry = context.Endpoint.RedeliveryClassifier.ShouldRetry(exception);
                await context.Acknowledgement.NackAsync(shouldRetry, CancellationToken.None).ConfigureAwait(false);
            }

            throw;
        }
    }
}
