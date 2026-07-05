using System;
using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging.Inbound;
using NATS.Client.JetStream;

namespace BrilliantMessaging.Transport.Nats.Inbound;

/// <summary>
/// JetStream acknowledgement adapter.
/// </summary>
public sealed class NatsMessageAcknowledgement : IMessageAcknowledgement
{
    private readonly Func<CancellationToken, Task<bool>> _deadLetterAsync;
    private readonly uint _deliveryAttempt;
    private readonly int _maxDeliver;
    private readonly INatsJSMsg<byte[]> _message;
    private readonly TimeSpan _nakDelay;
    private int _settled;

    /// <summary>
    /// Initializes a new instance of the <see cref="NatsMessageAcknowledgement" /> class.
    /// </summary>
    public NatsMessageAcknowledgement(
        INatsJSMsg<byte[]> message,
        TimeSpan nakDelay,
        uint deliveryAttempt,
        int maxDeliver,
        Func<CancellationToken, Task<bool>> deadLetterAsync
    )
    {
        _message = message ?? throw new ArgumentNullException(nameof(message));
        _nakDelay = nakDelay;
        _deliveryAttempt = deliveryAttempt == 0 ? 1 : deliveryAttempt;
        _maxDeliver = maxDeliver <= 0 ? 1 : maxDeliver;
        _deadLetterAsync = deadLetterAsync ?? throw new ArgumentNullException(nameof(deadLetterAsync));
    }

    /// <inheritdoc />
    public async Task AckAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref _settled, 1) != 0)
        {
            return;
        }

        await _message.AckAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task NackAsync(bool requeue, CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref _settled, 1) != 0)
        {
            return;
        }

        if (requeue && _deliveryAttempt < _maxDeliver)
        {
            AckOpts opts = new () { NakDelay = _nakDelay };
            await _message.NakAsync(opts, cancellationToken).ConfigureAwait(false);
            return;
        }

        var deadLettered = await _deadLetterAsync(cancellationToken).ConfigureAwait(false);
        AckOpts terminate = new ()
        {
            TerminateReason = deadLettered ?
                "Dead-lettered by Brilliant Messaging." :
                "Terminated by Brilliant Messaging."
        };
        await _message.AckTerminateAsync(terminate, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns the message to the stream for immediate redelivery without consuming the retry policy:
    /// no NAK delay, no MaxDeliver check, no dead-lettering. Intended for deliveries interrupted by
    /// shutdown rather than failed by the handler.
    /// </summary>
    public async Task RequeueAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref _settled, 1) != 0)
        {
            return;
        }

        await _message.NakAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
    }
}
