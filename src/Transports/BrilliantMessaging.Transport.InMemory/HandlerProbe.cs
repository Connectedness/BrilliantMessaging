using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace BrilliantMessaging.Transport.InMemory;

/// <summary>
/// Records every handler invocation and lets the caller decide whether a given invocation should fail or block.
/// Shared as a singleton across the per-delivery scopes that resolve the handlers.
/// </summary>
public sealed class HandlerProbe
{
    private readonly Lock _gate = new ();
    private readonly List<HandlerInvocation> _invocations = [];
    private readonly List<(int Count, TaskCompletionSource<bool> Source)> _waiters = [];

    /// <summary>
    /// A callback invoked for each delivery that returns the exception to throw, or <see langword="null" /> to
    /// complete successfully.
    /// </summary>
    public Func<HandlerInvocation, Exception?>? OnHandle { get; set; }

    /// <summary>
    /// When set, every handler awaits this gate (observing the delivery cancellation token) before it returns,
    /// letting the caller hold a delivery in flight.
    /// </summary>
    public TaskCompletionSource<bool>? Gate { get; set; }

    /// <summary>
    /// Gets the handler invocations recorded so far, in invocation order.
    /// </summary>
    public IReadOnlyList<HandlerInvocation> Invocations
    {
        get
        {
            lock (_gate)
            {
                return _invocations.ToArray();
            }
        }
    }

    /// <summary>
    /// Records an invocation, then optionally blocks on <see cref="Gate" /> and throws the exception returned by
    /// <see cref="OnHandle" />, if any.
    /// </summary>
    public async Task HandleAsync(
        string route,
        string endpointName,
        object message,
        uint deliveryAttempt,
        CancellationToken cancellationToken
    )
    {
        HandlerInvocation invocation = new (route, endpointName, message, (int) deliveryAttempt);
        List<TaskCompletionSource<bool>> ready = [];

        lock (_gate)
        {
            _invocations.Add(invocation);
            for (var index = _waiters.Count - 1; index >= 0; index--)
            {
                if (_invocations.Count >= _waiters[index].Count)
                {
                    ready.Add(_waiters[index].Source);
                    _waiters.RemoveAt(index);
                }
            }
        }

        foreach (var source in ready)
        {
            source.TrySetResult(true);
        }

        var gate = Gate;
        if (gate is not null)
        {
            await gate.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }

        var exception = OnHandle?.Invoke(invocation);
        if (exception is not null)
        {
            throw exception;
        }
    }

    /// <summary>
    /// Completes once at least <paramref name="count" /> handler invocations have been recorded.
    /// </summary>
    public async Task WaitForInvocationsAsync(int count, TimeSpan timeout)
    {
        Task task;
        lock (_gate)
        {
            if (_invocations.Count >= count)
            {
                return;
            }

            var source = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            _waiters.Add((count, source));
            task = source.Task;
        }

        await task.WaitAsync(timeout).ConfigureAwait(false);
    }
}
