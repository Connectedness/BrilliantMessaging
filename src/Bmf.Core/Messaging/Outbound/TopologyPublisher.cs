using System;
using System.Threading;
using System.Threading.Tasks;
using Bmf.Abstractions;

namespace Bmf.Core.Messaging.Outbound;

/// <summary>
/// A lightweight publisher bound to a single topology, returned by <see cref="MessagePublisher.ForTopology" />.
/// It forwards every publish to the underlying <see cref="MessagePublisher" /> with the bound topology name,
/// sparing callers from repeating it.
/// </summary>
public readonly struct TopologyPublisher
{
    private readonly string _topologyName;

    /// <summary>
    /// Initializes a new instance of the <see cref="TopologyPublisher" /> struct.
    /// </summary>
    /// <param name="router">The underlying publisher to forward to.</param>
    /// <param name="topologyName">The topology to bind to.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="router" /> is <see langword="null" />.</exception>
    /// <exception cref="ArgumentException">Thrown when <paramref name="topologyName" /> is null or whitespace.</exception>
    public TopologyPublisher(MessagePublisher router, string topologyName)
    {
        Router = router ?? throw new ArgumentNullException(nameof(router));
        _topologyName = !string.IsNullOrWhiteSpace(topologyName) ?
            topologyName :
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(topologyName));
    }

    /// <summary>
    /// Publishes a message against the bound topology, deriving the CloudEvents metadata from the message.
    /// </summary>
    /// <typeparam name="T">The message type, which must implement <see cref="ICloudEvent" />.</typeparam>
    /// <param name="message">The message to publish.</param>
    /// <param name="target">An explicit target, or <see langword="null" /> to resolve it from the topology by message type.</param>
    /// <param name="routingKey">An optional transport routing key.</param>
    /// <param name="cancellationToken">A token to observe while publishing.</param>
    /// <returns>A task that completes when the transport has accepted the message.</returns>
    public Task PublishMessageAsync<T>(
        T message,
        OutboundTarget? target = null,
        string? routingKey = null,
        CancellationToken cancellationToken = default
    ) where T : ICloudEvent
    {
        return Router.PublishMessageAsync(message, _topologyName, target, routingKey, cancellationToken);
    }

    /// <summary>
    /// Publishes a message against the bound topology with explicit CloudEvents metadata.
    /// </summary>
    /// <typeparam name="T">The message type.</typeparam>
    /// <param name="message">The message to publish.</param>
    /// <param name="metadata">The CloudEvents metadata to attach.</param>
    /// <param name="target">An explicit target, or <see langword="null" /> to resolve it from the topology by message type.</param>
    /// <param name="routingKey">An optional transport routing key.</param>
    /// <param name="cancellationToken">A token to observe while publishing.</param>
    /// <returns>A task that completes when the transport has accepted the message.</returns>
    public Task PublishMessageAsync<T>(
        T message,
        in CloudEventMetadata metadata,
        OutboundTarget? target = null,
        string? routingKey = null,
        CancellationToken cancellationToken = default
    )
    {
        return Router.PublishMessageAsync(message, in metadata, _topologyName, target, routingKey, cancellationToken);
    }

    /// <summary>
    /// Publishes an already-serialized message to an explicit target on the bound topology.
    /// </summary>
    /// <param name="message">The serialized message to publish.</param>
    /// <param name="target">The target to publish to.</param>
    /// <param name="cancellationToken">A token to observe while publishing.</param>
    /// <returns>A task that completes when the transport has accepted the message.</returns>
    public Task PublishRawAsync(
        SerializedMessage message,
        OutboundTarget target,
        CancellationToken cancellationToken = default
    )
    {
        return Router.PublishRawAsync(message, target, _topologyName, cancellationToken);
    }

    private MessagePublisher Router =>
        field ?? throw new InvalidOperationException("TopologyPublisher must not be the default instance");
}
