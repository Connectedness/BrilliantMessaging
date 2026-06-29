using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging.Inbound;
using BrilliantMessaging.Transport.InMemory.Inbound;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace BrilliantMessaging.Transport.InMemory;

/// <summary>
/// The process-local runtime state of an in-memory topology and the explicit support service tests use to inspect
/// it. The broker records every message routed to any topic (including dead-letter topics), dispatches deliveries
/// through per-consumer-route background workers, schedules retries through an injectable delay scheduler, and
/// owns the worker lifecycle for graceful shutdown.
/// </summary>
/// <remarks>
/// The recording exposed through <see cref="GetMessages" /> is a test/support facility and must not be relied on
/// by the normal runtime path. Broker state is process-local and non-durable: it never crosses a process boundary
/// and is discarded with the service provider that owns it.
/// </remarks>
public sealed class InMemoryBroker
{
    private readonly object _idleLock = new ();
    private readonly ILogger _logger;
    private readonly MessageDelegate _pipeline;

    private readonly ConcurrentDictionary<string, ConcurrentQueue<InMemoryTransportMessage>> _recordings =
        new (StringComparer.Ordinal);

    private readonly IReadOnlyList<InMemoryConsumerRoute> _routes;
    private readonly IReadOnlyDictionary<string, IReadOnlyList<InMemoryConsumerRoute>> _routesByTopic;
    private readonly IInMemoryDelayScheduler _scheduler;
    private readonly IServiceScopeFactory _serviceScopeFactory;
    private readonly TimeSpan _shutdownTimeout;
    private readonly string _topologyName;

    private volatile bool _acceptingWork = true;
    private TaskCompletionSource<bool> _idleSource = CreateCompletedIdleSource();
    private long _outstanding;
    private int _started;
    private CancellationTokenSource? _workerCancellation;
    private Task[] _workers = [];

    internal InMemoryBroker(
        string topologyName,
        IReadOnlyList<InMemoryConsumerRoute> routes,
        MessageDelegate pipeline,
        IServiceScopeFactory serviceScopeFactory,
        IInMemoryDelayScheduler scheduler,
        TimeSpan shutdownTimeout,
        ILogger? logger
    )
    {
        _topologyName = topologyName;
        _routes = routes;
        _pipeline = pipeline;
        _serviceScopeFactory = serviceScopeFactory;
        _scheduler = scheduler;
        _shutdownTimeout = shutdownTimeout;
        _logger = logger ?? NullLogger.Instance;

        Dictionary<string, IReadOnlyList<InMemoryConsumerRoute>> routesByTopic = new (StringComparer.Ordinal);
        foreach (var group in routes.GroupBy(static route => route.Topic, StringComparer.Ordinal))
        {
            routesByTopic[group.Key] = group.ToArray();
        }

        _routesByTopic = routesByTopic;
    }

    /// <summary>
    /// Gets the messages recorded for the given topic in the order they were routed, including messages that were
    /// consumed and messages republished by dead-lettering. Inspecting a dead-letter topic is simply calling this
    /// with the configured <c>DeadLetterTo</c> topic name.
    /// </summary>
    /// <param name="topic">The topic to inspect.</param>
    /// <returns>The recorded messages, or an empty list when nothing has been routed to the topic.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="topic" /> is null or whitespace.</exception>
    public IReadOnlyList<InMemoryTransportMessage> GetMessages(string topic)
    {
        if (string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(topic));
        }

        return _recordings.TryGetValue(topic, out var recorded) ? recorded.ToArray() : [];
    }

    /// <summary>
    /// Waits until the broker is idle — no queued deliveries, no in-flight handlers, and no scheduled retry
    /// deliveries remain — or until the timeout or cancellation, whichever comes first.
    /// </summary>
    /// <param name="timeout">The maximum time to wait, or <see cref="Timeout.InfiniteTimeSpan" /> to wait indefinitely.</param>
    /// <param name="cancellationToken">A token observed while waiting.</param>
    /// <returns>A task that completes once the broker is idle.</returns>
    /// <exception cref="TimeoutException">Thrown when the broker does not become idle within <paramref name="timeout" />.</exception>
    /// <exception cref="OperationCanceledException">Thrown when <paramref name="cancellationToken" /> is cancelled.</exception>
    public async Task DrainUntilIdleAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        using var timeoutCancellation = new CancellationTokenSource();
        if (timeout != Timeout.InfiniteTimeSpan)
        {
            timeoutCancellation.CancelAfter(timeout);
        }

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            timeoutCancellation.Token
        );

        while (true)
        {
            Task idleTask;
            lock (_idleLock)
            {
                if (_outstanding == 0)
                {
                    return;
                }

                idleTask = _idleSource.Task;
            }

            var waitTask = Task.Delay(Timeout.Infinite, linked.Token);
            var completed = await Task.WhenAny(idleTask, waitTask).ConfigureAwait(false);

            if (completed == idleTask)
            {
                continue;
            }

            cancellationToken.ThrowIfCancellationRequested();
            throw new TimeoutException(
                $"The in-memory topology '{_topologyName}' did not become idle within {timeout}."
            );
        }
    }

    /// <summary>
    /// Routes a published message to its topic: records it for inspection and enqueues a delivery onto each
    /// consumer route subscribed to the topic. Dispatch is always deferred to the background workers, never run
    /// inline.
    /// </summary>
    internal Task RouteAsync(
        string topic,
        ReadOnlyMemory<byte> body,
        IReadOnlyDictionary<string, object?> headers,
        string? contentType,
        string? messageId
    )
    {
        if (!_acceptingWork)
        {
            throw new InvalidOperationException(
                $"The in-memory topology '{_topologyName}' has been stopped and no longer accepts published messages."
            );
        }

        RouteCore(new InMemoryDelivery(topic, body, headers, contentType, messageId, attempt: 1));
        return Task.CompletedTask;
    }

    internal Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref _started, 1) != 0)
        {
            return Task.CompletedTask;
        }

        _workerCancellation = new CancellationTokenSource();
        var workerToken = _workerCancellation.Token;
        List<Task> workers = [];

        foreach (var route in _routes)
        {
            for (var worker = 0; worker < route.Concurrency; worker++)
            {
                workers.Add(Task.Run(() => RunWorkerAsync(route, workerToken), CancellationToken.None));
            }
        }

        _workers = workers.ToArray();
        return Task.CompletedTask;
    }

    internal async Task StopAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref _started, 0) == 0)
        {
            return;
        }

        // Stop accepting new published work, then let queued deliveries, in-flight handlers, and already scheduled
        // retry deliveries drain against the topology timeout before cancelling the remaining work.
        _acceptingWork = false;
        ExceptionDispatchInfo? cancellation = null;

        try
        {
            await DrainUntilIdleAsync(_shutdownTimeout, cancellationToken).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            _workerCancellation?.Cancel();
            try
            {
                await DrainUntilIdleAsync(Timeout.InfiniteTimeSpan, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                _logger.LogWarning(
                    exception,
                    "In-memory topology '{Topology}' did not become idle after shutdown cancellation.",
                    _topologyName
                );
            }
        }
        catch (OperationCanceledException exception)
        {
            _workerCancellation?.Cancel();
            cancellation = ExceptionDispatchInfo.Capture(exception);
        }
        finally
        {
            foreach (var route in _routes)
            {
                route.CompleteWriter();
            }

            try
            {
                await Task.WhenAll(_workers).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                _logger.LogWarning(
                    exception,
                    "In-memory topology '{Topology}' worker faulted during shutdown.",
                    _topologyName
                );
            }

            _workerCancellation?.Dispose();
            _workerCancellation = null;
        }

        cancellation?.Throw();
    }

    internal void CompleteDelivery()
    {
        DecrementOutstanding();
    }

    internal void AbandonDelivery()
    {
        DecrementOutstanding();
    }

    internal void HandleNack(
        InMemoryConsumerRoute route,
        InMemoryDelivery delivery,
        bool requeue,
        CancellationToken workerToken
    )
    {
        var policy = route.DeliveryPolicy;

        if (requeue && policy.Retry is { } retry && delivery.Attempt < retry.MaxAttempts)
        {
            ScheduleRetry(route, delivery, retry, workerToken);
            return;
        }

        // The delivery is either explicitly rejected, exhausted its attempts, or failed under the default drop
        // policy: republish it to the dead-letter topic when configured, otherwise drop it. Routing happens before
        // the decrement, so the outstanding count never momentarily reaches zero between the two.
        if (policy.DeadLetterTopic is { } deadLetterTopic)
        {
            RouteCore(delivery.RepublishTo(deadLetterTopic));
        }

        DecrementOutstanding();
    }

    private void RouteCore(InMemoryDelivery delivery)
    {
        Record(delivery.Topic, delivery.CreateMessage());

        if (!_routesByTopic.TryGetValue(delivery.Topic, out var routes))
        {
            return;
        }

        foreach (var route in routes)
        {
            IncrementOutstanding();
            if (!route.TryEnqueue(delivery))
            {
                // The route's queue is already completed (the topology is stopping): undo the count.
                DecrementOutstanding();
            }
        }
    }

    private void Record(string topic, InMemoryTransportMessage message)
    {
        var recorded = _recordings.GetOrAdd(topic, static _ => new ConcurrentQueue<InMemoryTransportMessage>());
        recorded.Enqueue(message);
    }

    private void ScheduleRetry(
        InMemoryConsumerRoute route,
        InMemoryDelivery delivery,
        InMemoryRetryPolicy retry,
        CancellationToken workerToken
    )
    {
        var delay = retry.Backoff.GetDelay(delivery.Attempt);
        var next = delivery.WithNextAttempt();

        // The delivery's outstanding count is kept alive across the scheduled delay, so a drain waits for the
        // retry. The retry runs on the worker cancellation token: a shutdown cancels the scheduled delay.
        _ = RunScheduledRetryAsync(route, next, delay, workerToken);
    }

    private async Task RunScheduledRetryAsync(
        InMemoryConsumerRoute route,
        InMemoryDelivery delivery,
        TimeSpan delay,
        CancellationToken workerToken
    )
    {
        try
        {
            await _scheduler.DelayAsync(delay, workerToken).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // The scheduled delay was cancelled (shutdown) or faulted: drop the retry.
            DecrementOutstanding();
            return;
        }

        if (!route.TryEnqueue(delivery))
        {
            // The route's queue was completed while the retry was pending: drop it.
            DecrementOutstanding();
        }
    }

    private async Task RunWorkerAsync(InMemoryConsumerRoute route, CancellationToken workerToken)
    {
        var reader = route.Reader;

        // Read with an uncancelled token so a stopping topology drains its already-queued deliveries before the
        // worker exits; the worker token only cancels the in-flight handler invocation.
        while (await reader.WaitToReadAsync(CancellationToken.None).ConfigureAwait(false))
        {
            while (reader.TryRead(out var delivery))
            {
                await ProcessDeliveryAsync(route, delivery, workerToken).ConfigureAwait(false);
            }
        }
    }

    private async Task ProcessDeliveryAsync(
        InMemoryConsumerRoute route,
        InMemoryDelivery delivery,
        CancellationToken workerToken
    )
    {
        var acknowledgement = new InMemoryMessageAcknowledgement(this, route, delivery, workerToken);

        if (workerToken.IsCancellationRequested)
        {
            await acknowledgement.NackAsync(requeue: true, CancellationToken.None).ConfigureAwait(false);
            return;
        }

        var transportMessage = delivery.CreateMessage();

        await using var scope = _serviceScopeFactory.CreateAsyncScope();

        try
        {
            var inspector = scope.ServiceProvider.GetRequiredService<CloudEventsInboundMessageInspector>();
            var inspectResult = await inspector
               .InspectAsync(transportMessage, workerToken)
               .ConfigureAwait(false);

            if (inspectResult is null)
            {
                throw new UnknownInboundMessageException(
                    route.Topic,
                    "(unrecognized)",
                    $"Inbound message from topic '{route.Topic}' was not recognized by the CloudEvents inspector."
                );
            }

            if (!route.TryGetEndpoint(inspectResult.Discriminator, out var endpoint))
            {
                throw new UnknownInboundMessageException(route.Topic, inspectResult.Discriminator);
            }

            if (endpoint.MessageType != inspectResult.MessageType &&
                !endpoint.MessageType.IsAssignableFrom(inspectResult.MessageType))
            {
                throw new UnknownInboundMessageException(
                    route.Topic,
                    inspectResult.Discriminator,
                    $"Inbound message discriminator '{inspectResult.Discriminator}' resolved to '{inspectResult.MessageType}', but endpoint '{endpoint.Name}' handles '{endpoint.MessageType}'."
                );
            }

            IncomingMessageContext context = new (
                transportMessage,
                endpoint,
                scope.ServiceProvider,
                acknowledgement,
                workerToken,
                inspectResult.MessageType,
                inspectResult.Items
            )
            {
                Message = inspectResult.Message
            };

            await _pipeline(context).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (workerToken.IsCancellationRequested)
        {
            await acknowledgement.NackAsync(requeue: true, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            _logger.LogError(
                exception,
                "In-memory inbound delivery failed for topic {Topic}.",
                route.Topic
            );

            // The acknowledgement settles at most once, so this is a no-op when the framework acknowledgement
            // middleware already settled inside the pipeline; it only matters for the pre-pipeline path
            // (unrecognized message, unknown endpoint) and for an unexpected failure in an outer middleware that
            // runs before settlement.
            await acknowledgement.NackAsync(requeue: false, CancellationToken.None).ConfigureAwait(false);
        }
    }

    private void IncrementOutstanding()
    {
        lock (_idleLock)
        {
            if (_outstanding == 0)
            {
                _idleSource = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            }

            _outstanding++;
        }
    }

    private void DecrementOutstanding()
    {
        lock (_idleLock)
        {
            _outstanding--;
            if (_outstanding == 0)
            {
                _idleSource.TrySetResult(true);
            }
        }
    }

    private static TaskCompletionSource<bool> CreateCompletedIdleSource()
    {
        var source = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        source.SetResult(true);
        return source;
    }
}
