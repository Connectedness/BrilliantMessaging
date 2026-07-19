using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Outbound;
using BrilliantMessaging.Transport.Nats.Inbound;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace BrilliantMessaging.Transport.Nats;

/// <summary>
/// Provisions or asserts JetStream streams and durable consumers for a compiled topology.
/// </summary>
public sealed class NatsTopologyProvisioner : ITopologyProvisioner
{
    // Topology provisioning is not a messaging operation, so it stays outside the OpenTelemetry messaging
    // conventions and keeps the brilliantmessaging.* tag scheme alongside its brilliantmessaging.outbound.topology.provisioning.* instruments.
    private const string TransportNameTagName = "brilliantmessaging.outbound.transport.name";

    private const string OutcomeTagName = "brilliantmessaging.outbound.outcome";

    // JetStream API error codes (ApiError.ErrCode), see https://docs.nats.io/reference/reference-protocols/nats_api_reference
    private const int StreamNotFoundErrorCode = 10059;

    private const int ConsumerNotFoundErrorCode = 10014;

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
        var outcome = "success";
        var activity =
            OutboundDiagnostics.ActivitySource.StartActivity("brilliantmessaging.outbound.topology.provision");
        var startedTimestamp = Stopwatch.GetTimestamp();
        KeyValuePair<string, object?>[] attemptTags =
        [
            new (TransportNameTagName, NatsTopology.TransportNameValue)
        ];

        OutboundDiagnostics.TopologyProvisioningAttempts.Add(1, attemptTags);
        activity?.SetTag(TransportNameTagName, NatsTopology.TransportNameValue);

        try
        {
            var jetStream = await _topology.GetJetStreamAsync(cancellationToken).ConfigureAwait(false);
            List<string> mismatches = [];

            foreach (var stream in _topology.Streams)
            {
                if (_topology.ProvisioningMode == NatsTopologyProvisioningMode.AssertOnly)
                {
                    try
                    {
                        var existing = await jetStream
                           .GetStreamAsync(stream.Name, cancellationToken: cancellationToken)
                           .ConfigureAwait(false);
                        ValidateStreamMatches(stream, existing.Info.Config, mismatches);
                    }
                    catch (NatsJSApiException exception) when (exception.Error.ErrCode == StreamNotFoundErrorCode)
                    {
                        mismatches.Add(
                            $"NATS stream '{stream.Name}' was not found on the server, but the topology declares it."
                        );
                    }

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
                    try
                    {
                        var existing = await jetStream
                           .GetConsumerAsync(consumer.StreamName, consumer.DurableName, cancellationToken)
                           .ConfigureAwait(false);
                        ValidateConsumerMatches(consumer, existing.Info.Config, mismatches);
                    }
                    // The consumer lookup fails with StreamNotFound when the whole stream is absent.
                    catch (NatsJSApiException exception)
                        when (exception.Error.ErrCode is ConsumerNotFoundErrorCode or StreamNotFoundErrorCode)
                    {
                        mismatches.Add(
                            $"NATS consumer '{consumer.DurableName}' was not found on stream '{consumer.StreamName}', but the topology declares it."
                        );
                    }

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

            activity?.SetStatus(ActivityStatusCode.Ok);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
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
                    new KeyValuePair<string, object?>(TransportNameTagName, NatsTopology.TransportNameValue),
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
                new (TransportNameTagName, NatsTopology.TransportNameValue),
                new (OutcomeTagName, outcome)
            ];
            var durationMilliseconds = (Stopwatch.GetTimestamp() - startedTimestamp) * 1000d / Stopwatch.Frequency;
            OutboundDiagnostics.TopologyProvisioningDuration.Record(durationMilliseconds, durationTags);
            activity?.SetTag(OutcomeTagName, outcome);
            activity?.Dispose();
        }
    }

    private static void ValidateStreamMatches(
        NatsStreamDefinition stream,
        StreamConfig existing,
        ICollection<string> mismatches
    )
    {
        var expected = ToStreamConfig(stream);

        var existingSubjects = existing.Subjects ?? [];
        var expectedSubjects = expected.Subjects ?? [];

        if (!existingSubjects.OrderBy(static subject => subject, StringComparer.Ordinal)
               .SequenceEqual(
                    expectedSubjects.OrderBy(static subject => subject, StringComparer.Ordinal),
                    StringComparer.Ordinal
                ))
        {
            mismatches.Add(
                $"NATS stream '{stream.Name}' has subjects [{string.Join(", ", existingSubjects)}] on the server, but the topology declares [{string.Join(", ", expectedSubjects)}]."
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

        if (stream.MaxAge is { } maxAge && existing.MaxAge != maxAge)
        {
            mismatches.Add(
                $"NATS stream '{stream.Name}' has max age '{existing.MaxAge}' on the server, but the topology declares '{maxAge}'."
            );
        }

        if (stream.MaxMessageSize is { } maxMessageSize && existing.MaxMsgSize != maxMessageSize)
        {
            mismatches.Add(
                $"NATS stream '{stream.Name}' has max message size {existing.MaxMsgSize} on the server, but the topology declares {maxMessageSize}."
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

        if (existing.MaxDeliver != consumer.ServerMaxDeliver)
        {
            mismatches.Add(
                $"NATS consumer '{consumer.DurableName}' has MaxDeliver {existing.MaxDeliver} on the server, but the topology declares {consumer.ServerMaxDeliver} (2 x MaxDeliver {consumer.MaxDeliver} for shutdown-interruption headroom)."
            );
        }

        if (existing.MaxAckPending != consumer.MaxAckPending)
        {
            mismatches.Add(
                $"NATS consumer '{consumer.DurableName}' has MaxAckPending {existing.MaxAckPending} on the server, but the topology declares {consumer.MaxAckPending}."
            );
        }

        if (!string.Equals(existing.FilterSubject, consumer.FilterSubject, StringComparison.Ordinal))
        {
            mismatches.Add(
                $"NATS consumer '{consumer.DurableName}' has filter subject '{existing.FilterSubject}' on the server, but the topology declares '{consumer.FilterSubject}'."
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
            // The configured MaxDeliver is enforced client-side for real handler failures; the server
            // headroom keeps shutdown-interrupted deliveries redeliverable (see
            // NatsInboundConsumer.ServerMaxDeliver).
            MaxDeliver = consumer.ServerMaxDeliver,
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
