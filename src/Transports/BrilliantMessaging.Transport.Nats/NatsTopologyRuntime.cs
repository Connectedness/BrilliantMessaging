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
            for (var i = 0; i < consumer.Concurrency; i++)
            {
                _workers.Add(RunConsumerAsync(consumer, _stopping.Token));
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
        catch (Exception exception)
        {
            _logger?.LogDebug(exception, "NATS topology '{Topology}' stopped with cancelled workers", _topology.Name);
        }
        finally
        {
            _workers.Clear();
            _stopping?.Dispose();
            _stopping = null;
        }
    }

    private async Task RunConsumerAsync(NatsInboundConsumer configuredConsumer, CancellationToken cancellationToken)
    {
        var jetStream = await _topology.GetJetStreamAsync(cancellationToken).ConfigureAwait(false);
        var consumer = await jetStream
           .GetConsumerAsync(configuredConsumer.StreamName, configuredConsumer.DurableName, cancellationToken)
           .ConfigureAwait(false);
        NatsJSConsumeOpts options = new ()
        {
            MaxMsgs = 512,
            Expires = TimeSpan.FromSeconds(30)
        };

        await foreach (var message in consumer
                          .ConsumeAsync<byte[]>(serializer: null, options, cancellationToken)
                          .ConfigureAwait(false))
        {
            try
            {
                await DispatchAsync(configuredConsumer, message, cancellationToken).ConfigureAwait(false);
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

    private async Task DispatchAsync(
        NatsInboundConsumer consumer,
        INatsJSMsg<byte[]> message,
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

        using var scope = _serviceScopeFactory.CreateScope();

        var inspector = scope.ServiceProvider.GetRequiredService<CloudEventsInboundMessageInspector>();
        InboundMessageInspectionResult? inspectResult;
        try
        {
            inspectResult = await inspector.InspectAsync(transportMessage, cancellationToken).ConfigureAwait(false);
        }
        catch (UnknownInboundMessageException)
        {
            await DeadLetterOrTerminateAsync(consumer, message, transportMessage, headers, cancellationToken)
               .ConfigureAwait(false);
            return;
        }

        if (inspectResult is null ||
            !consumer.EndpointsByDiscriminator.TryGetValue(inspectResult.Discriminator, out var endpoint))
        {
            await DeadLetterOrTerminateAsync(consumer, message, transportMessage, headers, cancellationToken)
               .ConfigureAwait(false);
            return;
        }

        if (endpoint.MessageType != inspectResult.MessageType &&
            !endpoint.MessageType.IsAssignableFrom(inspectResult.MessageType))
        {
            await DeadLetterOrTerminateAsync(consumer, message, transportMessage, headers, cancellationToken)
               .ConfigureAwait(false);
            return;
        }

        var deliveryAttempt = (uint) (message.Metadata?.NumDelivered ?? 1);
        var acknowledgement = new NatsMessageAcknowledgement(
            message,
            GetNakDelay(consumer, deliveryAttempt),
            deliveryAttempt,
            consumer.MaxDeliver,
            token => PublishDeadLetterAsync(consumer, message, headers, token)
        );

        using var progress = StartAckProgress(message, consumer.AckWait, cancellationToken);
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

        try
        {
            await _topology.Pipeline(context).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
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
    }

    private IDisposable? StartAckProgress(
        INatsJSMsg<byte[]> message,
        TimeSpan ackWait,
        CancellationToken cancellationToken
    )
    {
        if (!_topology.AckProgressEnabled)
        {
            return null;
        }

        var source = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var token = source.Token;
        var interval = TimeSpan.FromTicks(Math.Max(TimeSpan.FromSeconds(1).Ticks, ackWait.Ticks / 3));
        var task = Task.Run(
            async () =>
            {
                while (!token.IsCancellationRequested)
                {
                    try
                    {
                        await Task.Delay(interval, token).ConfigureAwait(false);
                        await message.AckProgressAsync(cancellationToken: token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        return;
                    }
                    catch (Exception exception)
                    {
                        _logger?.LogDebug(exception, "NATS AckProgress failed");
                    }
                }
            },
            CancellationToken.None
        );

        return new ProgressLease(source, task);
    }

    private async Task DeadLetterOrTerminateAsync(
        NatsInboundConsumer consumer,
        INatsJSMsg<byte[]> message,
        NatsTransportMessage transportMessage,
        IReadOnlyDictionary<string, object?> headers,
        CancellationToken cancellationToken
    )
    {
        _logger?.LogWarning(
            "NATS topology '{Topology}' consumer '{Consumer}' received a message with no matching handler; {Action}",
            _topology.Name,
            consumer.DurableName,
            consumer.DeadLetterSubject is null ? "terminating it" : "dead-lettering it"
        );

        // The pre-pipeline reject never reaches InboundDiagnosticsMiddleware, so the runtime owns this delivery's
        // messaging.client.consumed.messages measurement and carries the bounded error.type. An unrecognized
        // delivery classifies as _OTHER, matching ResolveErrorType(UnknownInboundMessageException).
        var tags = new TagList
        {
            { MessagingSemanticConventions.MessagingSystem, transportMessage.MessagingSystem },
            { MessagingSemanticConventions.MessagingOperationName, MessagingSemanticConventions.ProcessOperation },
            { MessagingSemanticConventions.MessagingDestinationName, transportMessage.Source },
            { MessagingSemanticConventions.ErrorType, MessagingSemanticConventions.ErrorTypeOther }
        };
        InboundDiagnostics.ConsumedMessages.Add(1, tags);

        await PublishDeadLetterAsync(consumer, message, headers, cancellationToken).ConfigureAwait(false);
        await message.AckTerminateAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
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

        var jetStream = await _topology.GetJetStreamAsync(cancellationToken).ConfigureAwait(false);
        var acknowledgement = await jetStream
           .PublishAsync(
                consumer.DeadLetterSubject,
                message.Data,
                serializer: null,
                opts: null,
                headers: ToNatsHeaders(headers),
                cancellationToken
            )
           .ConfigureAwait(false);
        acknowledgement.EnsureSuccess();
        return true;
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

    private sealed class ProgressLease : IDisposable
    {
        private readonly CancellationTokenSource _source;
        private readonly Task _task;

        public ProgressLease(CancellationTokenSource source, Task task)
        {
            _source = source;
            _task = task;
        }

        public void Dispose()
        {
            _source.Cancel();
            _source.Dispose();
            _ = _task.Exception;
        }
    }
}
