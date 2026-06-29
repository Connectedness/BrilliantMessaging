using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace BrilliantMessaging.Transport.InMemory.Tests.TestSupport;

/// <summary>
/// Records every handler invocation and lets a test decide whether a given invocation should fail or block. Shared
/// as a singleton across the per-delivery scopes that resolve the handlers.
/// </summary>
public sealed class HandlerProbe
{
    private readonly object _gate = new ();
    private readonly List<HandlerInvocation> _invocations = [];
    private readonly List<(int Count, TaskCompletionSource<bool> Source)> _waiters = [];

    /// <summary>
    /// A callback invoked for each delivery that returns the exception to throw, or <see langword="null" /> to
    /// complete successfully.
    /// </summary>
    public Func<HandlerInvocation, Exception?>? OnHandle { get; set; }

    /// <summary>
    /// When set, every handler awaits this gate (observing the delivery cancellation token) before it returns,
    /// letting a test hold a delivery in flight.
    /// </summary>
    public TaskCompletionSource<bool>? Gate { get; set; }

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

/// <summary>
/// A single recorded handler invocation.
/// </summary>
/// <param name="Route">The topic the delivery arrived on.</param>
/// <param name="EndpointName">The endpoint name that handled the delivery.</param>
/// <param name="Message">The deserialized message.</param>
/// <param name="DeliveryAttempt">The one-based delivery attempt.</param>
public sealed record HandlerInvocation(string Route, string EndpointName, object Message, int DeliveryAttempt);
