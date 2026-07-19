using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;
using BrilliantMessaging.Transport.Nats.Inbound;
using BrilliantMessaging.Transport.Nats.Outbound;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NATS.Client.Core;
using NATS.Client.JetStream;

namespace BrilliantMessaging.Transport.Nats;

/// <summary>
/// Pull-based JetStream runtime for NATS inbound consumers.
/// </summary>
public sealed class NatsTopologyRuntime : ITopologyRuntime
{
    private const string NatsMsgIdHeaderName = "Nats-Msg-Id";

    private static readonly TimeSpan ConsumerRecoveryDelay = TimeSpan.FromSeconds(1);

    private readonly CloudEventsInboundMessageInspector _inspector;
    private readonly ILogger<NatsTopologyRuntime>? _logger;
    private readonly IServiceScopeFactory _serviceScopeFactory;
    private readonly NatsTopology _topology;
    private readonly List<Task> _workers = [];
    private int _started;
    private CancellationTokenSource? _stopping;

    /// <summary>
    /// Initializes a new instance of the <see cref="NatsTopologyRuntime" /> class.
    /// </summary>
    public NatsTopologyRuntime(
        NatsTopology topology,
        IServiceScopeFactory serviceScopeFactory,
        ILogger<NatsTopologyRuntime>? logger = null
    )
    {
        _topology = topology ?? throw new ArgumentNullException(nameof(topology));
        _serviceScopeFactory = serviceScopeFactory ?? throw new ArgumentNullException(nameof(serviceScopeFactory));
        _logger = logger ?? NullLogger<NatsTopologyRuntime>.Instance;

        // The inspector must resolve CloudEvents types against the topology's effective contract registry;
        // the DI singleton only knows the globally mapped contracts, not topology-local dialects.
        _inspector = new CloudEventsInboundMessageInspector(_topology.MessageContractRegistry);
    }

    /// <inheritdoc />
    public string TopologyName => _topology.Name;

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref _started, 1) != 0)
        {
            return Task.CompletedTask;
        }

        _stopping = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        foreach (var consumer in _topology.Consumers)
        {
            var slots = new InFlightSlot[consumer.Concurrency];
            for (var i = 0; i < consumer.Concurrency; i++)
            {
                slots[i] = new InFlightSlot();
                _workers.Add(RunConsumerAsync(consumer, slots[i], _stopping.Token));
            }

            if (_topology.AckProgressEnabled)
            {
                _workers.Add(RunAckProgressAsync(consumer, slots, _stopping.Token));
            }
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref _started, 0) == 0)
        {
            return;
        }

        if (_stopping is not null)
        {
            await _stopping.CancelAsync().ConfigureAwait(false);
        }

        try
        {
            await Task.WhenAll(_workers).WaitAsync(_topology.ShutdownTimeout, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (TimeoutException)
        {
            _logger?.LogWarning(
                "NATS topology '{Topology}' workers did not stop within {ShutdownTimeout}; in-flight deliveries will be redelivered after AckWait",
                _topology.Name,
                _topology.ShutdownTimeout
            );
        }
        catch (OperationCanceledException exception)
        {
            // Workers are cancelled via the internal _stopping token, so their cancellation never matches
            // the StopAsync token filter above; it is the expected graceful shutdown outcome.
            _logger?.LogDebug(exception, "NATS topology '{Topology}' stopped with cancelled workers", _topology.Name);
        }
        catch (Exception exception)
        {
            _logger?.LogWarning(
                exception,
                "NATS topology '{Topology}' workers faulted during shutdown",
                _topology.Name
            );
        }
        finally
        {
            _workers.Clear();
            _stopping?.Dispose();
            _stopping = null;
        }
    }

    private async Task RunConsumerAsync(
        NatsInboundConsumer configuredConsumer,
        InFlightSlot slot,
        CancellationToken cancellationToken
    )
    {
        // MaxMsgs bounds the client-side buffer per worker. Buffered messages are not heartbeated (only the
        // in-flight one gets AckProgress), so a large buffer would let the tail exceed AckWait while waiting
        // for the sequential dispatch loop.
        NatsJSConsumeOpts options = new ()
        {
            MaxMsgs = configuredConsumer.MaxBufferedMessages,
            Expires = TimeSpan.FromSeconds(30)
        };

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var jetStream = await _topology.GetJetStreamAsync(cancellationToken).ConfigureAwait(false);
                var consumer = await jetStream
                   .GetConsumerAsync(configuredConsumer.StreamName, configuredConsumer.DurableName, cancellationToken)
                   .ConfigureAwait(false);

                await foreach (var message in consumer
                                  .ConsumeAsync<byte[]>(serializer: null, options, cancellationToken)
                                  .ConfigureAwait(false))
                {
                    try
                    {
                        await DispatchAsync(configuredConsumer, message, slot, cancellationToken)
                           .ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        throw;
                    }
                    catch (Exception exception)
                    {
                        _logger?.LogError(
                            exception,
                            "NATS topology '{Topology}' consumer '{Consumer}' failed while processing a message",
                            _topology.Name,
                            configuredConsumer.DurableName
                        );
                    }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
            {
                _logger?.LogWarning(
                    exception,
                    "NATS topology '{Topology}' consumer '{Consumer}' consume loop failed; restarting",
                    _topology.Name,
                    configuredConsumer.DurableName
                );
            }

            await Task.Delay(ConsumerRecoveryDelay, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task DispatchAsync(
        NatsInboundConsumer consumer,
        INatsJSMsg<byte[]> message,
        InFlightSlot slot,
        CancellationToken cancellationToken
    )
    {
        var headers = GetHeaders(message);
        var transportMessage = new NatsTransportMessage(
            message.Subject,
            message.Data ?? [],
            headers,
            GetHeader(headers, "content-type"),
            GetHeader(headers, "content-encoding"),
            GetHeader(headers, "message-id"),
            (uint) (message.Metadata?.NumDelivered ?? 1)
        );

        InboundMessageInspectionResult? inspectResult;
        try
        {
            inspectResult = await _inspector.InspectAsync(transportMessage, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            // Inspection is pure computation over the received headers, so any failure - unknown contract,
            // missing required attribute, malformed timestamp - is deterministic malformation. Redelivery
            // can never succeed; settling the message immediately prevents it from stranding once
            // MaxDeliver is exhausted.
            await DeadLetterOrTerminateAsync(
                    consumer,
                    message,
                    transportMessage,
                    headers,
                    exception,
                    cancellationToken
                )
               .ConfigureAwait(false);
            return;
        }

        if (inspectResult is null ||
            !consumer.EndpointsByDiscriminator.TryGetValue(inspectResult.Discriminator, out var endpoint))
        {
            await DeadLetterOrTerminateAsync(
                    consumer,
                    message,
                    transportMessage,
                    headers,
                    inspectionFailure: null,
                    cancellationToken
                )
               .ConfigureAwait(false);
            return;
        }

        if (endpoint.MessageType != inspectResult.MessageType &&
            !endpoint.MessageType.IsAssignableFrom(inspectResult.MessageType))
        {
            await DeadLetterOrTerminateAsync(
                    consumer,
                    message,
                    transportMessage,
                    headers,
                    inspectionFailure: null,
                    cancellationToken
                )
               .ConfigureAwait(false);
            return;
        }

        await using var scope = _serviceScopeFactory.CreateAsyncScope();

        var deliveryAttempt = (uint) (message.Metadata?.NumDelivered ?? 1);
        var acknowledgement = new NatsMessageAcknowledgement(
            message,
            GetNakDelay(consumer, deliveryAttempt),
            deliveryAttempt,
            consumer.MaxDeliver,
            token => PublishDeadLetterAsync(consumer, message, headers, token)
        );

        IncomingMessageContext context = new (
            transportMessage,
            endpoint,
            scope.ServiceProvider,
            acknowledgement,
            cancellationToken,
            inspectResult.MessageType,
            inspectResult.Items
        )
        {
            Message = inspectResult.Message
        };

        slot.Enter(message);
        try
        {
            await _topology.Pipeline(context).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Shutdown interrupted the delivery; without settling it the message would stall for the full
            // AckWait before another instance picks it up. Requeue bypasses the NAK delay and MaxDeliver
            // handling because the delivery was interrupted, not failed.
            await acknowledgement.RequeueAsync(CancellationToken.None).ConfigureAwait(false);
            throw;
        }
        catch
        {
            // Settles at most once, so this is a no-op when FrameworkMessageAcknowledgementMiddleware already
            // settled inside the pipeline; the safety net only matters for manual ack mode and for outer
            // middleware that fault before settlement. The rethrow lets the consumer loop log the failure.
            await acknowledgement.NackAsync(requeue: false, CancellationToken.None).ConfigureAwait(false);
            throw;
        }
        finally
        {
            slot.Exit();
        }
    }

    private async Task RunAckProgressAsync(
        NatsInboundConsumer consumer,
        InFlightSlot[] slots,
        CancellationToken cancellationToken
    )
    {
        var interval = TimeSpan.FromTicks(Math.Max(TimeSpan.FromSeconds(1).Ticks, consumer.AckWait.Ticks / 3));
        while (!cancellationToken.IsCancellationRequested)
        {
            await Task.Delay(interval, cancellationToken).ConfigureAwait(false);
            foreach (var slot in slots)
            {
                if (slot.Current is not { } inFlight)
                {
                    continue;
                }

                try
                {
                    await inFlight.AckProgressAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception exception)
                {
                    // Can race with settlement: progress on an already-settled delivery is rejected by the
                    // server, which is harmless here.
                    _logger?.LogDebug(exception, "NATS AckProgress failed");
                }
            }
        }
    }

    private async Task DeadLetterOrTerminateAsync(
        NatsInboundConsumer consumer,
        INatsJSMsg<byte[]> message,
        NatsTransportMessage transportMessage,
        IReadOnlyDictionary<string, object?> headers,
        Exception? inspectionFailure,
        CancellationToken cancellationToken
    )
    {
        var action = consumer.DeadLetterSubject is null ? "terminating it" : "dead-lettering it";
        if (inspectionFailure is null)
        {
            _logger?.LogWarning(
                "NATS topology '{Topology}' consumer '{Consumer}' received a message with no matching handler; {Action}",
                _topology.Name,
                consumer.DurableName,
                action
            );
        }
        else
        {
            _logger?.LogWarning(
                inspectionFailure,
                "NATS topology '{Topology}' consumer '{Consumer}' received a message that failed inbound inspection; {Action}",
                _topology.Name,
                consumer.DurableName,
                action
            );
        }

        // The pre-pipeline reject never reaches InboundDiagnosticsMiddleware, so the runtime owns this delivery's
        // messaging.client.consumed.messages measurement and carries the bounded error.type, classified through
        // the same ResolveErrorType mapping the middleware would apply. Unroutable deliveries without an
        // exception classify as _OTHER.
        var tags = new TagList
        {
            { MessagingSemanticConventions.MessagingSystem, transportMessage.MessagingSystem },
            { MessagingSemanticConventions.MessagingOperationName, MessagingSemanticConventions.ProcessOperation },
            { MessagingSemanticConventions.MessagingDestinationName, transportMessage.Source },
            {
                MessagingSemanticConventions.ErrorType,
                inspectionFailure is null ?
                    MessagingSemanticConventions.ErrorTypeOther :
                    MessagingSemanticConventions.ResolveErrorType(inspectionFailure)
            }
        };
        InboundDiagnostics.ConsumedMessages.Add(1, tags);

        var deadLettered = await PublishDeadLetterAsync(consumer, message, headers, cancellationToken)
           .ConfigureAwait(false);
        AckOpts terminate = new ()
        {
            TerminateReason = deadLettered ?
                NatsMessageAcknowledgement.DeadLetteredTerminateReason :
                NatsMessageAcknowledgement.TerminatedTerminateReason
        };
        await message.AckTerminateAsync(terminate, cancellationToken).ConfigureAwait(false);
    }

    private async Task<bool> PublishDeadLetterAsync(
        NatsInboundConsumer consumer,
        INatsJSMsg<byte[]> message,
        IReadOnlyDictionary<string, object?> headers,
        CancellationToken cancellationToken
    )
    {
        if (consumer.DeadLetterSubject is null)
        {
            return false;
        }

        // Deduplication is stream-wide, so republishing the original's Nats-Msg-Id inside the duplicate
        // window would suppress the dead-letter copy. The copy gets a derived id instead; the original
        // CloudEvents id remains available via the ce-id header.
        var natsHeaders = ToNatsHeaders(headers);
        natsHeaders.Remove(NatsMsgIdHeaderName);
        var messageId = GetDeadLetterMessageId(consumer, message);

        var jetStream = await _topology.GetJetStreamAsync(cancellationToken).ConfigureAwait(false);
        var acknowledgement = await jetStream
           .PublishAsync(
                consumer.DeadLetterSubject,
                message.Data,
                serializer: null,
                opts: messageId is null ? null : new NatsJSPubOpts { MsgId = messageId },
                headers: natsHeaders,
                cancellationToken
            )
           .ConfigureAwait(false);
        if (acknowledgement.Duplicate)
        {
            // A duplicate acknowledgement for the derived id proves an earlier publish already stored the
            // dead-letter copy - for example when the subsequent terminate failed and the redelivery
            // retries the sequence - so the delivery can proceed to termination.
            return true;
        }

        acknowledgement.EnsureSuccess();
        return true;
    }

    /// <summary>
    /// Derives the JetStream message id for a dead-letter copy. The id must differ from the original's
    /// <c>Nats-Msg-Id</c> so a stream-wide duplicate window cannot deduplicate the copy against the
    /// original, and it must be stable across redeliveries so a retried dead-letter publish after a
    /// failed terminate is recognized as already stored instead of creating a second copy. Returns
    /// <see langword="null" /> when the consumer has no dead-letter subject or no stable id source exists.
    /// </summary>
    public static string? GetDeadLetterMessageId(NatsInboundConsumer consumer, INatsJSMsg<byte[]> message)
    {
        if (consumer is null)
        {
            throw new ArgumentNullException(nameof(consumer));
        }

        if (message is null)
        {
            throw new ArgumentNullException(nameof(message));
        }

        if (consumer.DeadLetterSubject is null)
        {
            return null;
        }

        string? originalId = null;
        if (message.Headers is { } messageHeaders &&
            messageHeaders.TryGetValue(NatsMsgIdHeaderName, out var value))
        {
            var headerValue = value.ToString();
            originalId = string.IsNullOrWhiteSpace(headerValue) ? null : headerValue;
        }

        // The stream sequence identifies the stored message across redeliveries even when the producer
        // did not set a message id.
        if (originalId is null && message.Metadata is { } metadata)
        {
            originalId = $"{consumer.StreamName}:{metadata.Sequence.Stream}";
        }

        return originalId is null ?
            null :
            $"{originalId}:dlq:{consumer.DurableName}:{consumer.DeadLetterSubject}";
    }

    private static Dictionary<string, object?> GetHeaders(INatsJSMsg<byte[]> message)
    {
        Dictionary<string, object?> headers = new (StringComparer.Ordinal);
        if (message.Headers is not null)
        {
            foreach (var header in message.Headers)
            {
                headers[MapHeaderName(header.Key)] = header.Value.ToString();
            }
        }

        return headers;
    }

    private static NatsHeaders ToNatsHeaders(IReadOnlyDictionary<string, object?> headers)
    {
        NatsHeaders natsHeaders = new (headers.Count);
        foreach (var header in headers)
        {
            natsHeaders[MapHeaderNameForWire(header.Key)] = header.Value?.ToString();
        }

        return natsHeaders;
    }

    private static string MapHeaderName(string headerName)
    {
        return headerName.StartsWith(NatsOutboundTarget<object>.CloudEventsWireHeaderPrefix, StringComparison.Ordinal) ?
            $"{CloudEventsInboundMessageInspector.CloudEventsHeaderPrefix}{headerName.Substring(NatsOutboundTarget<object>.CloudEventsWireHeaderPrefix.Length)}" :
            headerName;
    }

    private static string MapHeaderNameForWire(string headerName)
    {
        return headerName.StartsWith(
            CloudEventsInboundMessageInspector.CloudEventsHeaderPrefix,
            StringComparison.Ordinal
        ) ?
            $"{NatsOutboundTarget<object>.CloudEventsWireHeaderPrefix}{headerName.Substring(CloudEventsInboundMessageInspector.CloudEventsHeaderPrefix.Length)}" :
            headerName;
    }

    private static string? GetHeader(IReadOnlyDictionary<string, object?> headers, string name)
    {
        return headers.TryGetValue(name, out var value) ? value?.ToString() : null;
    }

    private static TimeSpan GetNakDelay(NatsInboundConsumer consumer, uint deliveryAttempt)
    {
        var baseMilliseconds = Math.Max(100, Math.Min(consumer.AckWait.TotalMilliseconds / 2, 5000));
        var attempt = Math.Max(1, deliveryAttempt);
        var scaledMilliseconds = baseMilliseconds * Math.Pow(2, attempt - 1);
        var cappedMilliseconds = Math.Min(scaledMilliseconds, TimeSpan.FromSeconds(30).TotalMilliseconds);
        return TimeSpan.FromMilliseconds(cappedMilliseconds);
    }

    /// <summary>
    /// Holds the single message a sequential worker currently has in flight, so the per-consumer AckProgress
    /// loop can heartbeat it. Volatile access suffices: the loop only needs an eventually-current view.
    /// </summary>
    private sealed class InFlightSlot
    {
        private INatsJSMsg<byte[]>? _message;

        public INatsJSMsg<byte[]>? Current => Volatile.Read(ref _message);

        public void Enter(INatsJSMsg<byte[]> message) => Volatile.Write(ref _message, message);

        public void Exit() => Volatile.Write(ref _message, null);
    }
}
