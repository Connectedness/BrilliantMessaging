using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace BrilliantMessaging.Transport.InMemory;

/// <summary>
/// A hand-crafted <see cref="IInMemoryDelayScheduler" /> that captures every requested delay and releases them only
/// when the caller asks, so retry scheduling can be driven deterministically without sleeping.
/// </summary>
public sealed class ManualDelayScheduler : IInMemoryDelayScheduler
{
    private readonly Lock _gate = new ();
    private readonly List<Pending> _pending = [];
    private readonly List<TimeSpan> _requested = [];

    /// <summary>
    /// Gets the number of delays currently awaiting release.
    /// </summary>
    public int PendingCount
    {
        get
        {
            lock (_gate)
            {
                return _pending.Count;
            }
        }
    }

    /// <summary>
    /// Gets the delays that have been requested, in request order.
    /// </summary>
    public IReadOnlyList<TimeSpan> RequestedDelays
    {
        get
        {
            lock (_gate)
            {
                return _requested.ToArray();
            }
        }
    }

    /// <inheritdoc />
    public Task DelayAsync(TimeSpan delay, CancellationToken cancellationToken = default)
    {
        var source = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var registration = cancellationToken.Register(() => source.TrySetCanceled(cancellationToken));

        lock (_gate)
        {
            _pending.Add(new Pending(delay, source, registration));
            _requested.Add(delay);
        }

        return source.Task;
    }

    /// <summary>
    /// Releases every captured delay, completing the awaiting retries.
    /// </summary>
    public void ReleaseAll()
    {
        List<Pending> released;
        lock (_gate)
        {
            released = [.._pending];
            _pending.Clear();
        }

        foreach (var pending in released)
        {
            pending.Registration.Dispose();
            pending.Source.TrySetResult(true);
        }
    }

    /// <summary>
    /// Polls until at least <paramref name="count" /> delays are pending or the timeout elapses.
    /// </summary>
    public async Task WaitForPendingAsync(int count, TimeSpan timeout)
    {
        using var cancellation = new CancellationTokenSource(timeout);
        while (PendingCount < count)
        {
            if (cancellation.IsCancellationRequested)
            {
                throw new TimeoutException(
                    $"Expected at least {count} pending delay(s) within {timeout}, observed {PendingCount}."
                );
            }

            await Task.Delay(5, cancellation.Token).ConfigureAwait(false);
        }
    }

    private sealed record Pending(
        TimeSpan Delay,
        TaskCompletionSource<bool> Source,
        CancellationTokenRegistration Registration
    );
}
