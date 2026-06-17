using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Bmf.Core.Messaging.Inbound;

namespace Bmf.Transport.RabbitMq.Inbound;

/// <summary>
/// The active consumer runtime for a RabbitMQ topology that contains inbound endpoints. It opens consumer
/// channels, starts <c>BasicConsume</c> for each queue consumer, and drains in-flight handlers on stop. It does not
/// dispose the topology itself - the topology is a container-owned singleton whose connection may still be
/// used for publishing during graceful shutdown. It is registered as an <see cref="ITopologyRuntime" /> only
/// for topology instances that contain inbound endpoints, and is started by the shared
/// <see cref="TopologyRuntimeHostedService" /> after topology provisioning completes.
/// </summary>
public sealed class RabbitMqTopologyRuntime : ITopologyRuntime
{
    private readonly List<IChannel> _channels = [];
    private readonly List<ConsumerRegistration> _consumerRegistrations = [];
    private readonly ConcurrentDictionary<long, InFlightDelivery> _inFlightDeliveries = new ();
    private readonly ILogger _logger;
    private readonly IServiceScopeFactory _serviceScopeFactory;
    private readonly RabbitMqTopology _topology;
    private long _nextInFlightId;
    private int _started;
    private CancellationTokenSource? _stoppingCancellationTokenSource;

    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMqTopologyRuntime" /> class.
    /// </summary>
    /// <param name="topology">The topology whose consumers this runtime drives.</param>
    /// <param name="serviceScopeFactory">The factory used to create a DI scope per delivery.</param>
    /// <param name="logger">An optional logger; defaults to a no-op logger.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="topology" /> or <paramref name="serviceScopeFactory" /> is <see langword="null" />.</exception>
    public RabbitMqTopologyRuntime(
        RabbitMqTopology topology,
        IServiceScopeFactory serviceScopeFactory,
        ILogger<RabbitMqTopologyRuntime>? logger = null
    )
    {
        _topology = topology ?? throw new ArgumentNullException(nameof(topology));
        _serviceScopeFactory = serviceScopeFactory ?? throw new ArgumentNullException(nameof(serviceScopeFactory));
        _logger = logger ?? NullLogger<RabbitMqTopologyRuntime>.Instance;
    }

    /// <inheritdoc />
    public string TopologyName => _topology.Name;

    /// <summary>
    /// Opens the consumer channels, applies QoS, and starts consuming each queue. Subsequent calls are no-ops
    /// while the runtime is already started.
    /// </summary>
    /// <param name="cancellationToken">A token to observe while starting.</param>
    /// <returns>A task that completes once all consumers have started.</returns>
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref _started, 1) != 0)
        {
            return;
        }

        _stoppingCancellationTokenSource = new CancellationTokenSource();

        foreach (var consumerGroup in _topology.ConsumersByChannelGroup)
        {
            var channelGroup = consumerGroup.Key;
            var consumers = consumerGroup.ToArray();

            for (var index = 0; index < channelGroup.MaximumChannelCount; index++)
            {
                var channel = await _topology
                   .CreateChannelAsync(channelGroup.CreateChannelOptions(), cancellationToken)
                   .ConfigureAwait(false);
                await channel
                   .BasicQosAsync(
                        prefetchSize: 0,
                        prefetchCount: channelGroup.PrefetchCount,
                        global: false,
                        cancellationToken
                    )
                   .ConfigureAwait(false);
                _channels.Add(channel);

                foreach (var inboundConsumer in consumers)
                {
                    var consumer = new AsyncEventingBasicConsumer(channel);
                    consumer.ReceivedAsync += (_, eventArgs) => OnReceivedAsync(inboundConsumer, channel, eventArgs);
                    var consumerTag = await channel
                       .BasicConsumeAsync(
                            queue: inboundConsumer.QueueName,
                            autoAck: false,
                            consumerTag: string.Empty,
                            noLocal: false,
                            exclusive: false,
                            arguments: new Dictionary<string, object?>(0, StringComparer.Ordinal),
                            consumer: consumer,
                            cancellationToken: cancellationToken
                        )
                       .ConfigureAwait(false);
                    _consumerRegistrations.Add(new ConsumerRegistration(channel, consumerTag));
                }
            }
        }
    }

    /// <summary>
    /// Cancels the consumers, drains in-flight deliveries within the topology's shutdown timeout (requeuing any
    /// that do not finish in time), and disposes the consumer channels. The topology itself is not disposed.
    /// </summary>
    /// <param name="cancellationToken">A token to observe while stopping.</param>
    /// <returns>A task that completes once the runtime has stopped.</returns>
    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref _started, 0) == 0)
        {
            return;
        }

        _stoppingCancellationTokenSource?.Cancel();

        foreach (var registration in _consumerRegistrations)
        {
            try
            {
                await registration.Channel.BasicCancelAsync(
                        registration.ConsumerTag,
                        noWait: false,
                        cancellationToken
                    )
                   .ConfigureAwait(false);
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                _logger.LogWarning(
                    exception,
                    "RabbitMQ consumer cancel failed for consumer tag {ConsumerTag}",
                    registration.ConsumerTag
                );
            }
        }

        await DrainInFlightDeliveriesAsync(cancellationToken).ConfigureAwait(false);

        foreach (var channel in _channels)
        {
            if (channel is IAsyncDisposable asyncDisposable)
            {
                await asyncDisposable.DisposeAsync().ConfigureAwait(false);
            }
            else
            {
                channel.Dispose();
            }
        }

        _channels.Clear();
        _consumerRegistrations.Clear();
        _stoppingCancellationTokenSource?.Dispose();
        _stoppingCancellationTokenSource = null;
    }

    private async Task DrainInFlightDeliveriesAsync(CancellationToken cancellationToken)
    {
        if (_inFlightDeliveries.IsEmpty)
        {
            return;
        }

        var inFlightTasks = _inFlightDeliveries.Values.Select(static delivery => delivery.Completion).ToArray();
        var drainTask = Task.WhenAll(inFlightTasks);
        var timeoutTask = Task.Delay(_topology.ShutdownTimeout, cancellationToken);

        if (await Task.WhenAny(drainTask, timeoutTask).ConfigureAwait(false) == drainTask)
        {
            await drainTask.ConfigureAwait(false);
            return;
        }

        foreach (var delivery in _inFlightDeliveries.Values)
        {
            await delivery.Acknowledgement.NackAsync(requeue: true, CancellationToken.None).ConfigureAwait(false);
        }
    }

    private async Task OnReceivedAsync(
        RabbitMqInboundConsumer consumer,
        IChannel channel,
        BasicDeliverEventArgs eventArgs
    )
    {
        var acknowledgement = new RabbitMqMessageAcknowledgement(channel, eventArgs.DeliveryTag);
        var inFlightId = Interlocked.Increment(ref _nextInFlightId);
        var inFlight = new InFlightDelivery(acknowledgement);
        _inFlightDeliveries[inFlightId] = inFlight;

        try
        {
            await ProcessDeliveryAsync(consumer, acknowledgement, eventArgs).ConfigureAwait(false);
        }
        finally
        {
            _inFlightDeliveries.TryRemove(inFlightId, out _);
            inFlight.SetCompleted();
        }
    }

    private async Task ProcessDeliveryAsync(
        RabbitMqInboundConsumer consumer,
        RabbitMqMessageAcknowledgement acknowledgement,
        BasicDeliverEventArgs eventArgs
    )
    {
        var stoppingToken = _stoppingCancellationTokenSource?.Token ?? CancellationToken.None;

        if (stoppingToken.IsCancellationRequested)
        {
            await acknowledgement.NackAsync(requeue: true, CancellationToken.None).ConfigureAwait(false);
            return;
        }

        await using var scope = _serviceScopeFactory.CreateAsyncScope();
        using var linkedCancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(
            eventArgs.CancellationToken,
            stoppingToken
        );
        var cancellationToken = linkedCancellationTokenSource.Token;

        // The zero-copy body opt-in (CopyBody == false) aliases RabbitMQ.Client's pooled delivery buffer, which the
        // client only keeps valid for the duration of this delivery callback. The opt-in is therefore only sound
        // because we fully await the entire processing pipeline below before returning: the transport message and its
        // body never outlive this method. Any future change that offloads delivery past this callback (e.g. queuing
        // eventArgs to a background worker pool) would silently corrupt every zero-copy consumer and must instead force
        // a copy or reject zero-copy consumers at topology compilation.
        var transportMessage = new RabbitMqTransportMessage(
            consumer.QueueName,
            eventArgs.ConsumerTag,
            eventArgs.DeliveryTag,
            eventArgs.Redelivered,
            eventArgs.Exchange,
            eventArgs.RoutingKey,
            eventArgs.BasicProperties,
            eventArgs.Body,
            consumer.CopyBody
        );

        try
        {
            var inspector = (IInboundMessageInspector) scope.ServiceProvider.GetRequiredService(
                consumer.InspectorType
            );
            var inspectResult = await inspector.InspectAsync(transportMessage, cancellationToken).ConfigureAwait(false);

            if (!_topology.TryGetEndpoint(consumer.QueueName, inspectResult.Discriminator, out var endpoint))
            {
                throw new UnknownInboundMessageException(
                    consumer.QueueName,
                    inspectResult.Discriminator
                );
            }

            if (endpoint.MessageType != inspectResult.MessageType &&
                !endpoint.MessageType.IsAssignableFrom(inspectResult.MessageType))
            {
                throw new UnknownInboundMessageException(
                    consumer.QueueName,
                    inspectResult.Discriminator,
                    $"Inbound message discriminator '{inspectResult.Discriminator}' resolved to '{inspectResult.MessageType}', but endpoint '{endpoint.Name}' handles '{endpoint.MessageType}'."
                );
            }

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

            await _topology.Pipeline(context).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            await acknowledgement.NackAsync(requeue: true, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            _logger.LogError(
                exception,
                "RabbitMQ inbound delivery failed for queue {QueueName} and delivery tag {DeliveryTag}",
                consumer.QueueName,
                eventArgs.DeliveryTag
            );
            await acknowledgement.NackAsync(requeue: false, CancellationToken.None).ConfigureAwait(false);
        }
    }

    private sealed record ConsumerRegistration(IChannel Channel, string ConsumerTag);

    private sealed class InFlightDelivery
    {
        private readonly TaskCompletionSource<bool> _completion =
            new (TaskCreationOptions.RunContinuationsAsynchronously);

        public InFlightDelivery(RabbitMqMessageAcknowledgement acknowledgement)
        {
            Acknowledgement = acknowledgement;
        }

        public RabbitMqMessageAcknowledgement Acknowledgement { get; }

        public Task Completion => _completion.Task;

        public void SetCompleted()
        {
            _completion.TrySetResult(true);
        }
    }
}
