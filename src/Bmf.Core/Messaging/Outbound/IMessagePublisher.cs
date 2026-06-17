using System.Threading;
using System.Threading.Tasks;
using Bmf.Abstractions;

namespace Bmf.Core.Messaging.Outbound;

/// <summary>
/// Publishes messages to outbound targets. This is the application-facing publish surface; the framework
/// registers a singleton implementation backed by the topology registry.
/// </summary>
public interface IMessagePublisher
{
    /// <summary>
    /// Returns a publisher bound to the named topology so callers can omit the topology name on each publish.
    /// </summary>
    /// <param name="topologyName">The topology to bind to.</param>
    /// <returns>A publisher scoped to <paramref name="topologyName" />.</returns>
    TopologyPublisher ForTopology(string topologyName);

    /// <summary>
    /// Publishes a message, deriving the CloudEvents metadata from the message.
    /// </summary>
    /// <typeparam name="T">The message type, which must implement <see cref="ICloudEvent" />.</typeparam>
    /// <param name="message">The message to publish.</param>
    /// <param name="target">An explicit target, or <see langword="null" /> to resolve it from the topology by message type.</param>
    /// <param name="routingKey">An optional transport routing key.</param>
    /// <param name="cancellationToken">A token to observe while publishing.</param>
    /// <returns>A task that completes when the transport has accepted the message.</returns>
    Task PublishMessageAsync<T>(
        T message,
        OutboundTarget? target = null,
        string? routingKey = null,
        CancellationToken cancellationToken = default
    ) where T : ICloudEvent;

    /// <summary>
    /// Publishes a message with explicit CloudEvents metadata.
    /// </summary>
    /// <typeparam name="T">The message type.</typeparam>
    /// <param name="message">The message to publish.</param>
    /// <param name="metadata">The CloudEvents metadata to attach.</param>
    /// <param name="target">An explicit target, or <see langword="null" /> to resolve it from the topology by message type.</param>
    /// <param name="routingKey">An optional transport routing key.</param>
    /// <param name="cancellationToken">A token to observe while publishing.</param>
    /// <returns>A task that completes when the transport has accepted the message.</returns>
    Task PublishMessageAsync<T>(
        T message,
        in CloudEventMetadata metadata,
        OutboundTarget? target = null,
        string? routingKey = null,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Publishes an already-serialized message to an explicit target.
    /// </summary>
    /// <param name="message">The serialized message to publish.</param>
    /// <param name="target">The target to publish to.</param>
    /// <param name="cancellationToken">A token to observe while publishing.</param>
    /// <returns>A task that completes when the transport has accepted the message.</returns>
    Task PublishRawAsync(
        SerializedMessage message,
        OutboundTarget target,
        CancellationToken cancellationToken = default
    );
}
