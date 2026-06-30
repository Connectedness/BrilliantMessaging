using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Transport.Nats.Inbound;
using NATS.Client.JetStream.Models;

namespace BrilliantMessaging.Transport.Nats;

/// <summary>
/// Provisions or asserts JetStream streams and durable consumers for a compiled topology.
/// </summary>
public sealed class NatsTopologyProvisioner : ITopologyProvisioner
{
    private readonly NatsTopology _topology;

    /// <summary>
    /// Initializes a new instance of the <see cref="NatsTopologyProvisioner" /> class.
    /// </summary>
    public NatsTopologyProvisioner(NatsTopology topology)
    {
        _topology = topology ?? throw new ArgumentNullException(nameof(topology));
    }

    /// <inheritdoc />
    public async Task ProvisionAsync(CancellationToken cancellationToken = default)
    {
        var jetStream = await _topology.GetJetStreamAsync(cancellationToken).ConfigureAwait(false);
        List<string> mismatches = [];

        foreach (var stream in _topology.Streams)
        {
            if (_topology.ProvisioningMode == NatsTopologyProvisioningMode.AssertOnly)
            {
                var existing = await jetStream
                   .GetStreamAsync(stream.Name, cancellationToken: cancellationToken)
                   .ConfigureAwait(false);
                ValidateStreamMatches(stream, existing.Info.Config, mismatches);
                continue;
            }

            await jetStream
               .CreateOrUpdateStreamAsync(ToStreamConfig(stream), cancellationToken)
               .ConfigureAwait(false);
        }

        foreach (var consumer in _topology.Consumers)
        {
            if (_topology.ProvisioningMode == NatsTopologyProvisioningMode.AssertOnly)
            {
                var existing = await jetStream
                   .GetConsumerAsync(consumer.StreamName, consumer.DurableName, cancellationToken)
                   .ConfigureAwait(false);
                ValidateConsumerMatches(consumer, existing.Info.Config, mismatches);
                continue;
            }

            await jetStream
               .CreateOrUpdateConsumerAsync(consumer.StreamName, ToConsumerConfig(consumer), cancellationToken)
               .ConfigureAwait(false);
        }

        if (mismatches.Count > 0)
        {
            throw new TopologyValidationException(mismatches);
        }
    }

    private static void ValidateStreamMatches(
        NatsStreamDefinition stream,
        StreamConfig existing,
        ICollection<string> mismatches
    )
    {
        var expected = ToStreamConfig(stream);

        if (!existing.Subjects.OrderBy(static subject => subject, StringComparer.Ordinal)
               .SequenceEqual(
                    expected.Subjects.OrderBy(static subject => subject, StringComparer.Ordinal),
                    StringComparer.Ordinal
                ))
        {
            mismatches.Add(
                $"NATS stream '{stream.Name}' has subjects [{string.Join(", ", existing.Subjects)}] on the server, but the topology declares [{string.Join(", ", expected.Subjects)}]."
            );
        }

        if (existing.Storage != expected.Storage)
        {
            mismatches.Add(
                $"NATS stream '{stream.Name}' has storage '{existing.Storage}' on the server, but the topology declares '{expected.Storage}'."
            );
        }

        if (existing.Retention != expected.Retention)
        {
            mismatches.Add(
                $"NATS stream '{stream.Name}' has retention '{existing.Retention}' on the server, but the topology declares '{expected.Retention}'."
            );
        }

        if (existing.NumReplicas != expected.NumReplicas)
        {
            mismatches.Add(
                $"NATS stream '{stream.Name}' has {existing.NumReplicas} replicas on the server, but the topology declares {expected.NumReplicas}."
            );
        }

        if (stream.DuplicateWindow is { } duplicateWindow && existing.DuplicateWindow != duplicateWindow)
        {
            mismatches.Add(
                $"NATS stream '{stream.Name}' has duplicate window '{existing.DuplicateWindow}' on the server, but the topology declares '{duplicateWindow}'."
            );
        }
    }

    private static void ValidateConsumerMatches(
        NatsInboundConsumer consumer,
        ConsumerConfig existing,
        ICollection<string> mismatches
    )
    {
        if (existing.AckWait != consumer.AckWait)
        {
            mismatches.Add(
                $"NATS consumer '{consumer.DurableName}' has AckWait '{existing.AckWait}' on the server, but the topology declares '{consumer.AckWait}'."
            );
        }

        if (existing.MaxDeliver != consumer.MaxDeliver)
        {
            mismatches.Add(
                $"NATS consumer '{consumer.DurableName}' has MaxDeliver {existing.MaxDeliver} on the server, but the topology declares {consumer.MaxDeliver}."
            );
        }

        if (existing.MaxAckPending != consumer.MaxAckPending)
        {
            mismatches.Add(
                $"NATS consumer '{consumer.DurableName}' has MaxAckPending {existing.MaxAckPending} on the server, but the topology declares {consumer.MaxAckPending}."
            );
        }

        if (consumer.FilterSubject is { } filterSubject &&
            !string.Equals(existing.FilterSubject, filterSubject, StringComparison.Ordinal))
        {
            mismatches.Add(
                $"NATS consumer '{consumer.DurableName}' has filter subject '{existing.FilterSubject}' on the server, but the topology declares '{filterSubject}'."
            );
        }
    }

    /// <summary>
    /// Converts a Brilliant Messaging stream definition to a NATS.Net stream configuration.
    /// </summary>
    public static StreamConfig ToStreamConfig(NatsStreamDefinition stream)
    {
        StreamConfig config = new (stream.Name, [.. stream.Subjects])
        {
            Storage = stream.Storage == NatsStreamStorage.Memory ?
                StreamConfigStorage.Memory :
                StreamConfigStorage.File,
            Retention = stream.Retention switch
            {
                NatsStreamRetention.Interest => StreamConfigRetention.Interest,
                NatsStreamRetention.WorkQueue => StreamConfigRetention.Workqueue,
                _ => StreamConfigRetention.Limits
            },
            NumReplicas = stream.Replicas
        };

        if (stream.DuplicateWindow is { } duplicateWindow)
        {
            config.DuplicateWindow = duplicateWindow;
        }

        if (stream.MaxAge is { } maxAge)
        {
            config.MaxAge = maxAge;
        }

        if (stream.MaxMessageSize is { } maxMessageSize)
        {
            config.MaxMsgSize = maxMessageSize;
        }

        return config;
    }

    /// <summary>
    /// Converts a Brilliant Messaging consumer definition to a NATS.Net consumer configuration.
    /// </summary>
    public static ConsumerConfig ToConsumerConfig(NatsInboundConsumer consumer)
    {
        ConsumerConfig config = new (consumer.DurableName)
        {
            DurableName = consumer.DurableName,
            AckPolicy = ConsumerConfigAckPolicy.Explicit,
            AckWait = consumer.AckWait,
            MaxDeliver = consumer.MaxDeliver,
            MaxAckPending = consumer.MaxAckPending,
            DeliverPolicy = ConsumerConfigDeliverPolicy.All
        };

        if (consumer.FilterSubject is not null)
        {
            config.FilterSubject = consumer.FilterSubject;
        }

        return config;
    }
}
