using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Bmf.Abstractions;

namespace Bmf.Core.Messaging.Outbound;

/// <summary>
/// The default <see cref="IMessagePublisher" />. It resolves the outbound target for a message from the topology
/// registry (or uses an explicitly supplied target), then delegates to that target's instrumented publish path.
/// </summary>
public sealed class MessagePublisher : IMessagePublisher
{
    private readonly ITopologyRegistry _topologyRegistry;

    /// <summary>
    /// Initializes a new instance of the <see cref="MessagePublisher" /> class over a topology registry.
    /// </summary>
    /// <param name="topologyRegistry">The registry used to resolve topologies and their targets.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="topologyRegistry" /> is <see langword="null" />.</exception>
    [ActivatorUtilitiesConstructor]
    public MessagePublisher(ITopologyRegistry topologyRegistry)
    {
        _topologyRegistry = topologyRegistry ?? throw new ArgumentNullException(nameof(topologyRegistry));
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="MessagePublisher" /> class over a single topology.
    /// </summary>
    /// <param name="topology">The single topology to publish against.</param>
    public MessagePublisher(Topology topology)
        : this(new SingleTopologyRegistry(topology)) { }

    /// <summary>
    /// Returns a <see cref="TopologyPublisher" /> bound to the named topology so callers can publish without
    /// repeating the topology name on every call.
    /// </summary>
    /// <param name="topologyName">The topology to bind to.</param>
    /// <returns>A publisher scoped to <paramref name="topologyName" />.</returns>
    public TopologyPublisher ForTopology(string topologyName)
    {
        return new TopologyPublisher(this, topologyName);
    }

    /// <summary>
    /// Publishes a message against the default topology, deriving the CloudEvents metadata from the message.
    /// </summary>
    /// <typeparam name="T">The message type, which must implement <see cref="ICloudEvent" />.</typeparam>
    /// <param name="message">The message to publish.</param>
    /// <param name="target">An explicit target, or <see langword="null" /> to resolve it from the topology by message type.</param>
    /// <param name="routingKey">An optional transport routing key.</param>
    /// <param name="cancellationToken">A token to observe while publishing.</param>
    /// <returns>A task that completes when the transport has accepted the message.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="message" /> is <see langword="null" />.</exception>
    public Task PublishMessageAsync<T>(
        T message,
        OutboundTarget? target = null,
        string? routingKey = null,
        CancellationToken cancellationToken = default
    ) where T : ICloudEvent
    {
        if (message is null)
        {
            throw new ArgumentNullException(nameof(message));
        }

        var metadata = CloudEventMetadata.From(message);
        return PublishMessageAsync(message, in metadata, Topology.DefaultName, target, routingKey, cancellationToken);
    }

    /// <summary>
    /// Publishes a message against the default topology with explicit CloudEvents metadata.
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
        return PublishMessageAsync(message, in metadata, Topology.DefaultName, target, routingKey, cancellationToken);
    }

    /// <summary>
    /// Publishes an already-serialized message to an explicit target on the default topology.
    /// </summary>
    /// <param name="message">The serialized message to publish.</param>
    /// <param name="target">The target to publish to.</param>
    /// <param name="cancellationToken">A token to observe while publishing.</param>
    /// <returns>A task that completes when the transport has accepted the message.</returns>
    public async Task PublishRawAsync(
        SerializedMessage message,
        OutboundTarget target,
        CancellationToken cancellationToken = default
    )
    {
        await PublishRawAsync(message, target, Topology.DefaultName, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Publishes a message against a named topology, deriving the CloudEvents metadata from the message.
    /// </summary>
    /// <typeparam name="T">The message type, which must implement <see cref="ICloudEvent" />.</typeparam>
    /// <param name="message">The message to publish.</param>
    /// <param name="topologyName">The topology to resolve the target from.</param>
    /// <param name="target">An explicit target, or <see langword="null" /> to resolve it from the topology by message type.</param>
    /// <param name="routingKey">An optional transport routing key.</param>
    /// <param name="cancellationToken">A token to observe while publishing.</param>
    /// <returns>A task that completes when the transport has accepted the message.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="message" /> is <see langword="null" />.</exception>
    public Task PublishMessageAsync<T>(
        T message,
        string topologyName,
        OutboundTarget? target = null,
        string? routingKey = null,
        CancellationToken cancellationToken = default
    ) where T : ICloudEvent
    {
        if (message is null)
        {
            throw new ArgumentNullException(nameof(message));
        }

        var metadata = CloudEventMetadata.From(message);
        return PublishMessageAsync(message, in metadata, topologyName, target, routingKey, cancellationToken);
    }

    /// <summary>
    /// Publishes a message against a named topology with explicit CloudEvents metadata.
    /// </summary>
    /// <typeparam name="T">The message type.</typeparam>
    /// <param name="message">The message to publish.</param>
    /// <param name="metadata">The CloudEvents metadata to attach.</param>
    /// <param name="topologyName">The topology to resolve the target from.</param>
    /// <param name="target">An explicit target, or <see langword="null" /> to resolve it from the topology by message type.</param>
    /// <param name="routingKey">An optional transport routing key.</param>
    /// <param name="cancellationToken">A token to observe while publishing.</param>
    /// <returns>A task that completes when the transport has accepted the message.</returns>
    public Task PublishMessageAsync<T>(
        T message,
        in CloudEventMetadata metadata,
        string topologyName,
        OutboundTarget? target = null,
        string? routingKey = null,
        CancellationToken cancellationToken = default
    )
    {
        return PublishMessageCoreAsync(message, metadata, target, topologyName, routingKey, cancellationToken);
    }

    /// <summary>
    /// Publishes an already-serialized message to an explicit target on a named topology.
    /// </summary>
    /// <param name="message">The serialized message to publish.</param>
    /// <param name="target">The target to publish to.</param>
    /// <param name="topologyName">The topology the target is expected to belong to.</param>
    /// <param name="cancellationToken">A token to observe while publishing.</param>
    /// <returns>A task that completes when the transport has accepted the message.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="target" /> is <see langword="null" />.</exception>
    /// <exception cref="ArgumentException">Thrown when the serialized message has no body or headers.</exception>
    /// <exception cref="InvalidOperationException">Thrown when <paramref name="target" /> belongs to a different topology than <paramref name="topologyName" />.</exception>
    public async Task PublishRawAsync(
        SerializedMessage message,
        OutboundTarget target,
        string topologyName,
        CancellationToken cancellationToken = default
    )
    {
        if (target is null)
        {
            throw new ArgumentNullException(nameof(target));
        }

        if (message.Body is null)
        {
            throw new ArgumentException("The serialized message must provide a body.", nameof(message));
        }

        if (message.Headers is null)
        {
            throw new ArgumentException("The serialized message must provide headers.", nameof(message));
        }

        ValidateExplicitTargetTopology(target, topologyName);

        // The target layer owns publish diagnostics; the publisher only validates and delegates.
        await target.PublishSerializedAsync(message, cancellationToken).ConfigureAwait(false);
    }

    private async Task PublishMessageCoreAsync<T>(
        T message,
        CloudEventMetadata metadata,
        OutboundTarget? target,
        string topologyName,
        string? routingKey,
        CancellationToken cancellationToken
    )
    {
        if (message is null)
        {
            throw new ArgumentNullException(nameof(message));
        }

        var resolvedTarget = target ??
                             _topologyRegistry
                                .GetRequiredTopology(topologyName)
                                .GetRequiredTarget<T>();

        ValidateExplicitTargetTopology(resolvedTarget, topologyName, target is not null);
        if (resolvedTarget is not OutboundTarget<T> typedTarget)
        {
            throw new OutboundTargetTypeMismatchException(
                resolvedTarget.Name,
                typeof(T),
                resolvedTarget.MessageType
            );
        }

        var runtimeType = message.GetType();
        var discriminator = typedTarget.GetRequiredDiscriminator(runtimeType);
        var dataSchema = typedTarget.GetDataSchema(runtimeType);

        // Both branches funnel through OutboundTarget<T>.PublishCoreAsync, the single instrumented
        // publish path, so neither is wrapped in publisher-side diagnostics.
        if (string.IsNullOrWhiteSpace(routingKey))
        {
            await typedTarget
               .PublishAsync(message, in metadata, discriminator, dataSchema, cancellationToken)
               .ConfigureAwait(false);
        }
        else if (typedTarget is IOutboundRoutableTarget<T> routableTarget)
        {
            await routableTarget
               .PublishAsync(message, in metadata, discriminator, dataSchema, routingKey!, cancellationToken)
               .ConfigureAwait(false);
        }
        else
        {
            throw new OutboundTargetNotRoutableException(resolvedTarget.Name, typeof(T));
        }
    }

    private static void ValidateExplicitTargetTopology(
        OutboundTarget target,
        string topologyName,
        bool hasExplicitTarget = true
    )
    {
        if (!hasExplicitTarget ||
            string.Equals(topologyName, Topology.DefaultName, StringComparison.Ordinal) ||
            string.Equals(target.TopologyName, topologyName, StringComparison.Ordinal))
        {
            return;
        }

        throw new InvalidOperationException(
            $"Outbound target '{target.Name}' belongs to outbound topology '{target.TopologyName}', but publish requested outbound topology '{topologyName}'."
        );
    }
}
