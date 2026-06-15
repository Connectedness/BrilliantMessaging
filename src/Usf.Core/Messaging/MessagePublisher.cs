using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Usf.Abstractions;
using Usf.Core.Messaging.Errors;

namespace Usf.Core.Messaging;

public sealed class MessagePublisher : IMessagePublisher
{
    private readonly ITopologyRegistry _topologyRegistry;

    [ActivatorUtilitiesConstructor]
    public MessagePublisher(ITopologyRegistry topologyRegistry)
    {
        _topologyRegistry = topologyRegistry ?? throw new ArgumentNullException(nameof(topologyRegistry));
    }

    public MessagePublisher(Topology topology)
        : this(new SingleTopologyRegistry(topology)) { }

    public TopologyPublisher ForTopology(string topologyName)
    {
        return new TopologyPublisher(this, topologyName);
    }

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

    public async Task PublishRawAsync(
        SerializedMessage message,
        OutboundTarget target,
        CancellationToken cancellationToken = default
    )
    {
        await PublishRawAsync(message, target, Topology.DefaultName, cancellationToken).ConfigureAwait(false);
    }

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
