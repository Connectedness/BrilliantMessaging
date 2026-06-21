using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Bmf.Core.Messaging;
using Bmf.Core.Messaging.Outbound;
using RabbitMQ.Client;

namespace Bmf.Transport.RabbitMq;

/// <summary>
/// Provisions the broker resources (exchanges, queues, and bindings) of a single <see cref="RabbitMqTopology" />.
/// It runs as an <see cref="ITopologyProvisioner" /> so that the shared topology-provisioning hosted service
/// declares all broker resources before any topology runtime starts.
/// </summary>
public sealed class RabbitMqTopologyProvisioner : ITopologyProvisioner
{
    // Topology provisioning is not a messaging operation, so it stays outside the OpenTelemetry messaging
    // conventions and keeps the bmf.* tag scheme alongside its bmf.outbound.topology.provisioning.* instruments.
    private const string TransportNameTagName = "bmf.outbound.transport.name";

    private const string OutcomeTagName = "bmf.outbound.outcome";

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
        var activity = OutboundDiagnostics.ActivitySource.StartActivity("bmf.outbound.topology.provision");
        var startedTimestamp = Stopwatch.GetTimestamp();
        KeyValuePair<string, object?>[] attemptTags =
        [
            new (TransportNameTagName, "rabbitmq")
        ];

        OutboundDiagnostics.TopologyProvisioningAttempts.Add(1, attemptTags);
        activity?.SetTag(TransportNameTagName, "rabbitmq");

        try
        {
            await using var channel = await _topology.CreateChannelAsync(cancellationToken).ConfigureAwait(false);

            foreach (var exchange in _topology.Exchanges)
            {
                await ProvisionExchangeAsync(channel, exchange, cancellationToken).ConfigureAwait(false);
            }

            foreach (var queue in _topology.Queues)
            {
                await ProvisionQueueAsync(channel, queue, cancellationToken).ConfigureAwait(false);
            }

            foreach (var binding in _topology.Bindings)
            {
                await ProvisionBindingAsync(channel, binding, cancellationToken).ConfigureAwait(false);
            }

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

    private static Task ProvisionBindingAsync(
        IChannel channel,
        RabbitMqBindingDefinition binding,
        CancellationToken cancellationToken
    )
    {
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
            _ => throw new ArgumentOutOfRangeException(
                nameof(exchange),
                exchange.DeclareMode,
                "Unsupported declare mode."
            )
        };
    }

    private static Task ProvisionQueueAsync(
        IChannel channel,
        RabbitMqQueueDefinition queue,
        CancellationToken cancellationToken
    )
    {
        var arguments = CreateMutableArguments(queue.Arguments);

        return queue.DeclareMode switch
        {
            RabbitMqDeclareMode.Skip => Task.CompletedTask,
            RabbitMqDeclareMode.Passive => channel.QueueDeclarePassiveAsync(queue.Name, cancellationToken),
            RabbitMqDeclareMode.Active => channel.QueueDeclareAsync(
                queue.Name,
                queue.Durable,
                queue.Exclusive,
                queue.AutoDelete,
                arguments,
                cancellationToken: cancellationToken
            ),
            _ => throw new ArgumentOutOfRangeException(nameof(queue), queue.DeclareMode, "Unsupported declare mode.")
        };
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
