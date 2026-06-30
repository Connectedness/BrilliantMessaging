using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using BrilliantMessaging.Transport.InMemory.Inbound;

namespace BrilliantMessaging.Transport.InMemory;

/// <summary>
/// The compiled runtime form of an in-memory consumer: the topic it subscribes to, the worker concurrency, the
/// delivery policy, and the endpoints it dispatches to keyed by CloudEvents discriminator. Each route owns an
/// unbounded background queue; a topic with several routes fans each published message out to every route.
/// </summary>
public sealed class InMemoryConsumerRoute
{
    private readonly Channel<InMemoryDelivery> _channel;

    internal InMemoryConsumerRoute(
        string topic,
        int concurrency,
        InMemoryDeliveryPolicy deliveryPolicy,
        IReadOnlyDictionary<string, InMemoryInboundEndpoint> endpointsByDiscriminator
    )
    {
        Topic = topic;
        Concurrency = concurrency;
        DeliveryPolicy = deliveryPolicy;
        EndpointsByDiscriminator = endpointsByDiscriminator;
        _channel = Channel.CreateUnbounded<InMemoryDelivery>(
            new UnboundedChannelOptions
            {
                SingleReader = concurrency == 1,
                SingleWriter = false,
                AllowSynchronousContinuations = false
            }
        );
    }

    /// <summary>
    /// Gets the topic the route consumes.
    /// </summary>
    public string Topic { get; }

    /// <summary>
    /// Gets the number of background workers processing the route's deliveries.
    /// </summary>
    public int Concurrency { get; }

    /// <summary>
    /// Gets the route's failure-handling policy.
    /// </summary>
    public InMemoryDeliveryPolicy DeliveryPolicy { get; }

    /// <summary>
    /// Gets the endpoints the route dispatches to, keyed by CloudEvents discriminator.
    /// </summary>
    public IReadOnlyDictionary<string, InMemoryInboundEndpoint> EndpointsByDiscriminator { get; }

    internal ChannelReader<InMemoryDelivery> Reader => _channel.Reader;

    internal bool TryEnqueue(InMemoryDelivery delivery)
    {
        return _channel.Writer.TryWrite(delivery);
    }

    internal void CompleteWriter()
    {
        _channel.Writer.TryComplete();
    }

    internal bool TryGetEndpoint(string discriminator, [NotNullWhen(true)] out InMemoryInboundEndpoint? endpoint)
    {
        return EndpointsByDiscriminator.TryGetValue(discriminator, out endpoint);
    }
}
