using System;
using System.Collections.Generic;

namespace BrilliantMessaging.Transport.Nats.Inbound;

/// <summary>
/// Compiled durable consumer runtime metadata.
/// </summary>
public sealed class NatsInboundConsumer
{
    /// <summary>
    /// Initializes a new instance of the <see cref="NatsInboundConsumer" /> class.
    /// </summary>
    public NatsInboundConsumer(
        string streamName,
        string durableName,
        string? filterSubject,
        int concurrency,
        TimeSpan ackWait,
        int maxDeliver,
        int maxAckPending,
        string? deadLetterSubject,
        IReadOnlyDictionary<string, NatsInboundEndpoint> endpointsByDiscriminator
    )
    {
        StreamName = streamName;
        DurableName = durableName;
        FilterSubject = filterSubject;
        Concurrency = concurrency;
        AckWait = ackWait;
        MaxDeliver = maxDeliver;
        MaxAckPending = maxAckPending;
        DeadLetterSubject = deadLetterSubject;
        EndpointsByDiscriminator = endpointsByDiscriminator;
    }

    /// <summary>
    /// Gets the stream name.
    /// </summary>
    public string StreamName { get; }

    /// <summary>
    /// Gets the durable consumer name.
    /// </summary>
    public string DurableName { get; }

    /// <summary>
    /// Gets the optional filter subject.
    /// </summary>
    public string? FilterSubject { get; }

    /// <summary>
    /// Gets the consumer processing concurrency.
    /// </summary>
    public int Concurrency { get; }

    /// <summary>
    /// Gets JetStream AckWait.
    /// </summary>
    public TimeSpan AckWait { get; }

    /// <summary>
    /// Gets JetStream MaxDeliver.
    /// </summary>
    public int MaxDeliver { get; }

    /// <summary>
    /// Gets JetStream MaxAckPending.
    /// </summary>
    public int MaxAckPending { get; }

    /// <summary>
    /// Gets the dead-letter subject.
    /// </summary>
    public string? DeadLetterSubject { get; }

    /// <summary>
    /// Gets endpoints by CloudEvents discriminator.
    /// </summary>
    public IReadOnlyDictionary<string, NatsInboundEndpoint> EndpointsByDiscriminator { get; }
}
