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
    internal const string DeadLetteredTerminateReason = "Dead-lettered by Brilliant Messaging.";
    internal const string TerminatedTerminateReason = "Terminated by Brilliant Messaging.";

    internal const string InterruptedDeadLetteredTerminateReason =
        "Dead-lettered by Brilliant Messaging (shutdown interruption).";

    internal const string InterruptedTerminateReason =
        "Terminated by Brilliant Messaging (shutdown interruption).";

    /// <summary>
    /// NAK delay for shutdown-interrupted deliveries. An undelayed NAK is redelivered instantly, often
    /// into the stopping instance's still-draining pull buffer, where the cancelled pipeline interrupts
    /// it again and burns through the server-side redelivery headroom. The short delay lets the stopping
    /// instance quit its pull loops so the redelivery lands on a surviving or restarted instance, while
    /// staying far below a typical AckWait.
    /// </summary>
    internal static readonly TimeSpan ShutdownRequeueDelay = TimeSpan.FromSeconds(1);

    private readonly int _deadLetterAfterDeliveryAttempt;

    private readonly Func<CancellationToken, Task<bool>> _deadLetterAsync;
    private readonly uint _deliveryAttempt;
    private readonly int _maxDeliver;
    private readonly INatsJSMsg<byte[]> _message;
    private readonly TimeSpan _nakDelay;
    private readonly CancellationToken _shutdownToken;
    private int _settled;

    /// <summary>
    /// Initializes a new instance of the <see cref="NatsMessageAcknowledgement" /> class.
    /// </summary>
    public NatsMessageAcknowledgement(
        INatsJSMsg<byte[]> message,
        TimeSpan nakDelay,
        uint deliveryAttempt,
        int deadLetterAfterDeliveryAttempt,
        int maxDeliver,
        Func<CancellationToken, Task<bool>> deadLetterAsync,
        CancellationToken shutdownToken = default
    )
    {
        _message = message ?? throw new ArgumentNullException(nameof(message));
        _nakDelay = nakDelay;
        _deliveryAttempt = deliveryAttempt == 0 ? 1 : deliveryAttempt;
        _deadLetterAfterDeliveryAttempt = deadLetterAfterDeliveryAttempt <= 0 ? 1 : deadLetterAfterDeliveryAttempt;
        _maxDeliver = maxDeliver <= 0 ? 1 : maxDeliver;
        _deadLetterAsync = deadLetterAsync ?? throw new ArgumentNullException(nameof(deadLetterAsync));
        _shutdownToken = shutdownToken;
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
        // The framework acknowledgement middleware settles interrupted auto-ack deliveries through this
        // method; when shutdown is in progress the delivery was interrupted rather than failed, so it
        // must not consume a retry delay or be dead-lettered while server headroom remains.
        if (requeue && _shutdownToken.IsCancellationRequested)
        {
            await RequeueAsync(cancellationToken).ConfigureAwait(false);
            return;
        }

        if (Interlocked.Exchange(ref _settled, 1) != 0)
        {
            return;
        }

        if (requeue && _deliveryAttempt < _deadLetterAfterDeliveryAttempt)
        {
            AckOpts opts = new () { NakDelay = _nakDelay };
            await _message.NakAsync(opts, cancellationToken).ConfigureAwait(false);
            return;
        }

        var deadLettered = await _deadLetterAsync(cancellationToken).ConfigureAwait(false);
        AckOpts terminate = new ()
        {
            TerminateReason = deadLettered ?
                DeadLetteredTerminateReason :
                TerminatedTerminateReason
        };
        await _message.AckTerminateAsync(terminate, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Settles a delivery that was interrupted by shutdown rather than failed by the handler: a NAK with
    /// <see cref="ShutdownRequeueDelay" /> (no retry backoff or client-side dead-letter check) while the
    /// server-side redelivery headroom lasts. Once the server would stop redelivering, the message is
    /// dead-lettered or terminated instead - a NAK at that point would strand it without redelivery or
    /// dead-letter copy.
    /// </summary>
    public async Task RequeueAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref _settled, 1) != 0)
        {
            return;
        }

        if (_deliveryAttempt < _maxDeliver)
        {
            AckOpts opts = new () { NakDelay = ShutdownRequeueDelay };
            await _message.NakAsync(opts, cancellationToken).ConfigureAwait(false);
            return;
        }

        var deadLettered = await _deadLetterAsync(cancellationToken).ConfigureAwait(false);
        AckOpts terminate = new ()
        {
            TerminateReason = deadLettered ?
                InterruptedDeadLetteredTerminateReason :
                InterruptedTerminateReason
        };
        await _message.AckTerminateAsync(terminate, cancellationToken).ConfigureAwait(false);
    }
}
