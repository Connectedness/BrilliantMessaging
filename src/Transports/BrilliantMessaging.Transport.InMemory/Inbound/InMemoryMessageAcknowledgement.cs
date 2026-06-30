using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging.Inbound;

namespace BrilliantMessaging.Transport.InMemory.Inbound;

/// <summary>
/// The in-memory <see cref="IMessageAcknowledgement" />. It is the adapter between the inbound pipeline and the
/// consumer's delivery policy: <see cref="AckAsync" /> completes the delivery, <see cref="NackAsync" /> with
/// requeue schedules another attempt through the retry/backoff policy, and <see cref="NackAsync" /> without
/// requeue dead-letters or drops the delivery. It settles at most once, so duplicate settlement is a no-op.
/// </summary>
public sealed class InMemoryMessageAcknowledgement : IMessageAcknowledgement
{
    private readonly InMemoryBroker _broker;
    private readonly InMemoryDelivery _delivery;
    private readonly InMemoryConsumerRoute _route;
    private readonly CancellationToken _workerToken;
    private int _settled;

    internal InMemoryMessageAcknowledgement(
        InMemoryBroker broker,
        InMemoryConsumerRoute route,
        InMemoryDelivery delivery,
        CancellationToken workerToken
    )
    {
        _broker = broker;
        _route = route;
        _delivery = delivery;
        _workerToken = workerToken;
    }

    /// <inheritdoc />
    public Task AckAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref _settled, 1) != 0)
        {
            return Task.CompletedTask;
        }

        _broker.CompleteDelivery();
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task NackAsync(bool requeue, CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref _settled, 1) != 0)
        {
            return Task.CompletedTask;
        }

        // A negative acknowledgement that arrives while the worker token is cancelled is part of a graceful
        // shutdown that is cancelling remaining work: drop the delivery rather than retrying or dead-lettering it.
        if (_workerToken.IsCancellationRequested)
        {
            _broker.AbandonDelivery();
            return Task.CompletedTask;
        }

        _broker.HandleNack(_route, _delivery, requeue, _workerToken);
        return Task.CompletedTask;
    }
}
