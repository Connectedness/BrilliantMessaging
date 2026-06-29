using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Outbound;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace BrilliantMessaging.Transport.RabbitMq;

/// <summary>
/// Provisions the broker resources (exchanges, queues, and bindings) of a single <see cref="RabbitMqTopology" />.
/// It runs as an <see cref="ITopologyProvisioner" /> so that the shared topology-provisioning hosted service
/// declares all broker resources before any topology runtime starts.
/// </summary>
public sealed class RabbitMqTopologyProvisioner : ITopologyProvisioner
{
    // Topology provisioning is not a messaging operation, so it stays outside the OpenTelemetry messaging
    // conventions and keeps the brilliantmessaging.* tag scheme alongside its brilliantmessaging.outbound.topology.provisioning.* instruments.
    private const string TransportNameTagName = "brilliantmessaging.outbound.transport.name";

    private const string OutcomeTagName = "brilliantmessaging.outbound.outcome";

    private readonly RabbitMqTopology _topology;

    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMqTopologyProvisioner" /> class.
    /// </summary>
    /// <param name="topology">The topology whose broker resources are provisioned.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="topology" /> is <see langword="null" />.</exception>
    public RabbitMqTopologyProvisioner(RabbitMqTopology topology)
    {
        _topology = topology ?? throw new ArgumentNullException(nameof(topology));
    }

    /// <inheritdoc />
    public async Task ProvisionAsync(CancellationToken cancellationToken = default)
    {
        var outcome = "success";
        var activity =
            OutboundDiagnostics.ActivitySource.StartActivity("brilliantmessaging.outbound.topology.provision");
        var startedTimestamp = Stopwatch.GetTimestamp();
        KeyValuePair<string, object?>[] attemptTags =
        [
            new (TransportNameTagName, "rabbitmq")
        ];

        OutboundDiagnostics.TopologyProvisioningAttempts.Add(1, attemptTags);
        activity?.SetTag(TransportNameTagName, "rabbitmq");

        try
        {
            // Provisioning runs on a single, sequentially used channel, but several idempotency paths
            // (a Delete-mode queue or binding that is already absent on the broker) swallow a NOT_FOUND
            // and continue — and a NOT_FOUND is the broker having closed the channel underneath us. A
            // bounded pool of one self-heals that: each step acquires a lease, and a channel the broker
            // shut down is discarded on release so the next acquire opens a fresh one. A healthy channel
            // is returned to the pool and reused, so in the common case this is still one channel for the
            // whole run. Acquiring per step (rather than holding one lease across the loop) is what makes
            // the renewal work — channel health is only evaluated at acquire/release.
            await using var channelPool = new DefaultRabbitMqChannelPool(1, _topology.CreateChannelAsync);

            foreach (var exchange in _topology.Exchanges)
            {
                await using var lease = await channelPool.AcquireAsync(cancellationToken).ConfigureAwait(false);
                await ProvisionExchangeAsync(lease.Channel, exchange, cancellationToken).ConfigureAwait(false);
            }

            foreach (var queue in _topology.Queues)
            {
                await using var lease = await channelPool.AcquireAsync(cancellationToken).ConfigureAwait(false);
                await ProvisionQueueAsync(lease.Channel, queue, cancellationToken).ConfigureAwait(false);
            }

            await ProvisionBindingsAsync(
                channelPool,
                _topology.Bindings,
                _topology.Queues,
                _topology.Exchanges,
                cancellationToken
            ).ConfigureAwait(false);

            activity?.SetStatus(ActivityStatusCode.Ok);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Only a cancellation of the caller's token is a graceful, non-error cancellation. An
            // OperationCanceledException raised while the caller's token is not signalled is a genuine
            // provisioning failure and falls through to the failure path below.
            outcome = "cancelled";
            activity?.SetTag(OutcomeTagName, outcome);
            throw;
        }
        catch
        {
            outcome = "failure";
            OutboundDiagnostics.TopologyProvisioningFailures.Add(
                1,
                new[]
                {
                    new KeyValuePair<string, object?>(TransportNameTagName, "rabbitmq"),
                    new KeyValuePair<string, object?>(OutcomeTagName, outcome)
                }
            );
            activity?.SetStatus(ActivityStatusCode.Error);
            activity?.SetTag(OutcomeTagName, outcome);
            throw;
        }
        finally
        {
            KeyValuePair<string, object?>[] durationTags =
            [
                new (TransportNameTagName, "rabbitmq"),
                new (OutcomeTagName, outcome)
            ];
            var durationMilliseconds = (Stopwatch.GetTimestamp() - startedTimestamp) * 1000d / Stopwatch.Frequency;
            OutboundDiagnostics.TopologyProvisioningDuration.Record(durationMilliseconds, durationTags);
            activity?.SetTag(OutcomeTagName, outcome);
            activity?.Dispose();
        }
    }

    private static async Task ProvisionBindingsAsync(
        IRabbitMqChannelPool channelPool,
        IReadOnlyList<RabbitMqBindingDefinition> bindings,
        IReadOnlyList<RabbitMqQueueDefinition> queues,
        IReadOnlyList<RabbitMqExchangeDefinition> exchanges,
        CancellationToken cancellationToken
    )
    {
        // Build the set of Delete-mode queue names once so each binding can be checked without a per-binding scan.
        // By the time the binding loop runs, the preceding queue loop has already deleted these queues, so any
        // binding whose destination is in this set is skipped entirely — RabbitMQ removes bindings automatically
        // on queue delete, so an explicit unbind is redundant and a re-bind would target a soon-to-be-absent queue.
        HashSet<string> deleteQueueNames = new (StringComparer.Ordinal);

        foreach (var queue in queues)
        {
            if (queue.DeclareMode == RabbitMqDeclareMode.Delete)
            {
                deleteQueueNames.Add(queue.Name);
            }
        }

        // Build the matching set for Delete-mode exchanges. The preceding exchange loop has already deleted these
        // exchanges and the broker has cascade-removed the bindings owned by them (both as source and as
        // destination), so any binding that names a deleted exchange on either end is skipped in both passes: an
        // Active re-bind would target an absent exchange, and a Delete unbind would be redundant.
        HashSet<string> deleteExchangeNames = new (StringComparer.Ordinal);

        foreach (var exchange in exchanges)
        {
            if (exchange.DeclareMode == RabbitMqDeclareMode.Delete)
            {
                deleteExchangeNames.Add(exchange.Name);
            }
        }

        // Two passes guarantee routing continuity: all Active/Skip bindings first (creating/re-asserting
        // bindings, including a new ex → orders-v2 bind), then all Delete bindings (running the unbinds,
        // including the old ex → orders-v1 unbind). The exchange is never without a binding mid-provisioning
        // regardless of the order the user writes the binding calls in the topology.
        foreach (var binding in bindings)
        {
            if (binding.BindingMode is RabbitMqBindingMode.Active or RabbitMqBindingMode.Skip)
            {
                await using var lease = await channelPool.AcquireAsync(cancellationToken).ConfigureAwait(false);
                await ProvisionBindingAsync(
                    lease.Channel,
                    binding,
                    deleteQueueNames,
                    deleteExchangeNames,
                    cancellationToken
                ).ConfigureAwait(false);
            }
        }

        foreach (var binding in bindings)
        {
            if (binding.BindingMode == RabbitMqBindingMode.Delete)
            {
                await using var lease = await channelPool.AcquireAsync(cancellationToken).ConfigureAwait(false);
                await ProvisionBindingAsync(
                    lease.Channel,
                    binding,
                    deleteQueueNames,
                    deleteExchangeNames,
                    cancellationToken
                ).ConfigureAwait(false);
            }
        }
    }

    private static Task ProvisionBindingAsync(
        IChannel channel,
        RabbitMqBindingDefinition binding,
        HashSet<string> deleteQueueNames,
        HashSet<string> deleteExchangeNames,
        CancellationToken cancellationToken
    )
    {
        // A queue binding whose destination queue is in Delete mode is skipped entirely, regardless of the
        // binding's own mode. RabbitMQ auto-removes bindings on queue delete, so an explicit unbind is
        // redundant, and an explicit re-bind would target a queue the preceding queue loop just deleted.
        if (binding is RabbitMqQueueBindingDefinition { QueueName: var skipQueueName } &&
            deleteQueueNames.Contains(skipQueueName))
        {
            return Task.CompletedTask;
        }

        // A binding that names a Delete-mode exchange on either end is skipped entirely. The preceding exchange
        // loop has already deleted the exchange and the broker has cascade-removed its bindings, so an Active
        // re-bind would target an absent exchange and a Delete unbind would be redundant. This covers both
        // a queue binding's source exchange and an exchange binding's source or destination.
        if (deleteExchangeNames.Contains(binding.SourceExchangeName))
        {
            return Task.CompletedTask;
        }

        if (binding is RabbitMqExchangeBindingDefinition skipExchangeBinding &&
            deleteExchangeNames.Contains(skipExchangeBinding.DestinationExchangeName))
        {
            return Task.CompletedTask;
        }

        var arguments = CreateMutableArguments(binding.Arguments);

        return binding switch
        {
            RabbitMqQueueBindingDefinition { BindingMode: RabbitMqBindingMode.Skip } =>
                Task.CompletedTask,
            RabbitMqQueueBindingDefinition { BindingMode: RabbitMqBindingMode.Active } queueBinding =>
                channel.QueueBindAsync(
                    queueBinding.QueueName,
                    queueBinding.SourceExchangeName,
                    queueBinding.RoutingKey,
                    arguments,
                    false,
                    cancellationToken
                ),
            RabbitMqQueueBindingDefinition { BindingMode: RabbitMqBindingMode.Delete } queueBinding =>
                UnbindQueueBindingAsync(channel, queueBinding, arguments, cancellationToken),
            RabbitMqExchangeBindingDefinition { BindingMode: RabbitMqBindingMode.Skip } =>
                Task.CompletedTask,
            RabbitMqExchangeBindingDefinition { BindingMode: RabbitMqBindingMode.Active } exchangeBinding =>
                channel.ExchangeBindAsync(
                    exchangeBinding.DestinationExchangeName,
                    exchangeBinding.SourceExchangeName,
                    exchangeBinding.RoutingKey,
                    arguments,
                    false,
                    cancellationToken
                ),
            RabbitMqExchangeBindingDefinition { BindingMode: RabbitMqBindingMode.Delete } exchangeBinding =>
                UnbindExchangeBindingAsync(channel, exchangeBinding, arguments, cancellationToken),
            RabbitMqQueueBindingDefinition queueBinding => throw new ArgumentOutOfRangeException(
                nameof(binding),
                queueBinding.BindingMode,
                "Unsupported binding mode."
            ),
            RabbitMqExchangeBindingDefinition exchangeBinding => throw new ArgumentOutOfRangeException(
                nameof(binding),
                exchangeBinding.BindingMode,
                "Unsupported binding mode."
            ),
            _ => throw new ArgumentOutOfRangeException(nameof(binding), binding, "Unsupported binding type.")
        };
    }

    private static async Task UnbindQueueBindingAsync(
        IChannel channel,
        RabbitMqQueueBindingDefinition queueBinding,
        IDictionary<string, object?> arguments,
        CancellationToken cancellationToken
    )
    {
        try
        {
            await channel.QueueUnbindAsync(
                queueBinding.QueueName,
                queueBinding.SourceExchangeName,
                queueBinding.RoutingKey,
                arguments,
                cancellationToken
            ).ConfigureAwait(false);
        }
        catch (OperationInterruptedException ex) when (IsBrokerNotFound(ex))
        {
            // A not-found error means the binding (or its queue or exchange) is already absent on the broker.
            // The desired state — binding absent — is already achieved, so Delete mode is idempotent across
            // restarts. Other broker errors propagate unchanged.
        }
    }

    private static async Task UnbindExchangeBindingAsync(
        IChannel channel,
        RabbitMqExchangeBindingDefinition exchangeBinding,
        IDictionary<string, object?> arguments,
        CancellationToken cancellationToken
    )
    {
        try
        {
            await channel.ExchangeUnbindAsync(
                exchangeBinding.DestinationExchangeName,
                exchangeBinding.SourceExchangeName,
                exchangeBinding.RoutingKey,
                arguments,
                false,
                cancellationToken
            ).ConfigureAwait(false);
        }
        catch (OperationInterruptedException ex) when (IsBrokerNotFound(ex))
        {
            // A not-found error means the binding (or its source or destination exchange) is already absent on
            // the broker. The desired state — binding absent — is already achieved, so Delete mode is idempotent
            // across restarts. Other broker errors propagate unchanged.
        }
    }

    private static Task ProvisionExchangeAsync(
        IChannel channel,
        RabbitMqExchangeDefinition exchange,
        CancellationToken cancellationToken
    )
    {
        var arguments = CreateMutableArguments(exchange.Arguments);

        return exchange.DeclareMode switch
        {
            RabbitMqDeclareMode.Skip => Task.CompletedTask,
            RabbitMqDeclareMode.Passive => channel.ExchangeDeclarePassiveAsync(exchange.Name, cancellationToken),
            RabbitMqDeclareMode.Active => channel.ExchangeDeclareAsync(
                exchange.Name,
                exchange.Type,
                exchange.Durable,
                exchange.AutoDelete,
                arguments,
                cancellationToken: cancellationToken
            ),
            RabbitMqDeclareMode.Delete => DeleteExchangeAsync(channel, exchange.Name, cancellationToken),
            _ => throw new ArgumentOutOfRangeException(
                nameof(exchange),
                exchange.DeclareMode,
                "Unsupported declare mode."
            )
        };
    }

    private static async Task ProvisionQueueAsync(
        IChannel channel,
        RabbitMqQueueDefinition queue,
        CancellationToken cancellationToken
    )
    {
        switch (queue.DeclareMode)
        {
            case RabbitMqDeclareMode.Skip:
                return;
            case RabbitMqDeclareMode.Passive:
                await channel.QueueDeclarePassiveAsync(queue.Name, cancellationToken).ConfigureAwait(false);
                return;
            case RabbitMqDeclareMode.Active:
                var arguments = CreateMutableArguments(queue.Arguments);
                await channel
                   .QueueDeclareAsync(
                        queue.Name,
                        queue.Durable,
                        queue.Exclusive,
                        queue.AutoDelete,
                        arguments,
                        cancellationToken: cancellationToken
                    )
                   .ConfigureAwait(false);
                return;
            case RabbitMqDeclareMode.Delete:
                await DeleteQueueAsync(channel, queue.Name, cancellationToken).ConfigureAwait(false);
                return;
            default:
                throw new ArgumentOutOfRangeException(nameof(queue), queue.DeclareMode, "Unsupported declare mode.");
        }
    }

    private static async Task DeleteQueueAsync(
        IChannel channel,
        string queueName,
        CancellationToken cancellationToken
    )
    {
        // Quorum queues (the default queue type) do not support the ifUnused or ifEmpty flags on queue.delete,
        // so the "drain first" safety check is implemented as a pre-delete passive declare: if the queue has
        // messages, we throw before deleting. In the introduce → drain → delete workflow the binding is already
        // removed, so no new messages arrive between the check and the delete.
        try
        {
            var declareResult = await channel
               .QueueDeclarePassiveAsync(queueName, cancellationToken)
               .ConfigureAwait(false);

            if (declareResult.MessageCount > 0)
            {
                throw new InvalidOperationException(
                    $"Queue '{queueName}' cannot be deleted because it still has {declareResult.MessageCount} message(s). Drain the queue before setting its declare mode to Delete."
                );
            }

            await channel
               .QueueDeleteAsync(
                    queueName,
                    ifUnused: false,
                    ifEmpty: false,
                    cancellationToken: cancellationToken
                )
               .ConfigureAwait(false);
        }
        catch (OperationInterruptedException ex) when (IsBrokerNotFound(ex))
        {
            // A 404 NOT_FOUND means the queue is already absent on the broker — whether on the passive declare
            // or on the delete itself, since the queue can be removed (operator action, concurrent deploy)
            // between the two calls. The desired state — resource absent — is already achieved, so Delete mode
            // is idempotent across restarts.
        }
    }

    private static bool IsBrokerNotFound(OperationInterruptedException exception)
    {
        return exception.ShutdownReason is { ReplyCode: Constants.NotFound };
    }

    private static async Task DeleteExchangeAsync(
        IChannel channel,
        string exchangeName,
        CancellationToken cancellationToken
    )
    {
        // Exchange deletion is unconditional (ifUnused: false). Unlike a queue, an exchange holds no messages, so
        // there is nothing to drain before deleting, and the broker cascade-removes the bindings owned by the
        // deleted exchange. The binding skip-set in ProvisionBindingsAsync relies on this cascade: it leaves a
        // Delete exchange's outgoing bindings in place on the assumption the delete removes them, which an
        // ifUnused: true delete would refuse to do (it would reject the delete while the exchange still has any
        // source binding). Routing continuity across a swap is instead preserved by the binding two-pass
        // (create-before-destroy) and the multi-deploy introduce → swap → delete workflow.
        try
        {
            await channel
               .ExchangeDeleteAsync(exchangeName, ifUnused: false, cancellationToken: cancellationToken)
               .ConfigureAwait(false);
        }
        catch (OperationInterruptedException ex) when (IsBrokerNotFound(ex))
        {
            // A 404 NOT_FOUND means the exchange is already absent on the broker. The desired state — resource
            // absent — is already achieved, so Delete mode is idempotent across restarts. Other broker errors
            // propagate unchanged.
        }
    }

    private static IDictionary<string, object?> CreateMutableArguments(IReadOnlyDictionary<string, object?> arguments)
    {
        Dictionary<string, object?> mutableArguments = new (arguments.Count, StringComparer.Ordinal);

        foreach (var argument in arguments)
        {
            mutableArguments[argument.Key] = argument.Value;
        }

        return mutableArguments;
    }
}
