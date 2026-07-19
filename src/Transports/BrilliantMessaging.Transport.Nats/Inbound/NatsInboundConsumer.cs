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
        int maxBufferedMessages,
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
        MaxBufferedMessages = maxBufferedMessages;
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
    /// Gets the MaxDeliver value provisioned on the JetStream consumer: twice <see cref="MaxDeliver" />.
    /// Real handler failures are dead-lettered client-side once <see cref="MaxDeliver" /> attempts are
    /// exhausted; the extra server-side headroom lets deliveries that were interrupted by shutdown be
    /// redelivered instead of being dead-lettered or stranded, while still bounding redelivery of
    /// messages whose consumer dies without settling them.
    /// </summary>
    public int ServerMaxDeliver => GetServerMaxDeliver(MaxDeliver);

    /// <summary>
    /// Gets JetStream MaxAckPending.
    /// </summary>
    public int MaxAckPending { get; }

    /// <summary>
    /// Gets the number of messages each worker buffers client-side per pull request.
    /// </summary>
    public int MaxBufferedMessages { get; }

    /// <summary>
    /// Gets the dead-letter subject.
    /// </summary>
    public string? DeadLetterSubject { get; }

    /// <summary>
    /// Gets endpoints by CloudEvents discriminator.
    /// </summary>
    public IReadOnlyDictionary<string, NatsInboundEndpoint> EndpointsByDiscriminator { get; }

    /// <summary>
    /// Derives the server-side MaxDeliver (twice the configured value, saturating at
    /// <see cref="int.MaxValue" />) so the provisioner and the acknowledgement adapter apply the same
    /// shutdown-interruption headroom.
    /// </summary>
    public static int GetServerMaxDeliver(int maxDeliver)
    {
        return maxDeliver > int.MaxValue / 2 ? int.MaxValue : maxDeliver * 2;
    }
}
