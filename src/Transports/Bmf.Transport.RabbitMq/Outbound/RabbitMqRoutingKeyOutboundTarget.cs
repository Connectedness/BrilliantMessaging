using System;
using System.Threading;
using System.Threading.Tasks;
using Bmf.Abstractions;
using Bmf.Core.Messaging;
using Bmf.Core.Messaging.Outbound;

namespace Bmf.Transport.RabbitMq.Outbound;

/// <summary>
/// The base for routing-key-based RabbitMQ outbound targets (direct and topic exchanges). It implements
/// <see cref="IOutboundRoutableTarget{TMessage}" />, exposing per-publish routing-key overloads, and resolves the
/// routing key from either a fixed key or a per-message factory supplied at construction.
/// </summary>
/// <typeparam name="TMessage">The message type the target publishes.</typeparam>
public abstract class RabbitMqRoutingKeyOutboundTarget<TMessage>
    : RabbitMqOutboundTarget<TMessage>, IOutboundRoutableTarget<TMessage>
{
    private readonly string? _constantRoutingKey;
    private readonly Func<TMessage, string>? _routingKeyFactory;

    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMqRoutingKeyOutboundTarget{TMessage}" /> class. Exactly
    /// one of <paramref name="constantRoutingKey" /> or <paramref name="routingKeyFactory" /> must be supplied.
    /// </summary>
    /// <param name="name">The logical name of the target.</param>
    /// <param name="serializer">The serializer used to turn messages into CloudEvents envelopes.</param>
    /// <param name="messageContractRegistry">The registry used to resolve discriminators and data schemas.</param>
    /// <param name="topologyName">The name of the topology the target belongs to.</param>
    /// <param name="channelGroup">The channel group that supplies publish channels.</param>
    /// <param name="exchangeName">The name of the exchange messages are published to.</param>
    /// <param name="isMandatory">Whether published messages are sent with the AMQP mandatory flag.</param>
    /// <param name="constantRoutingKey">A fixed routing key, or <see langword="null" /> when a factory is used.</param>
    /// <param name="routingKeyFactory">A per-message routing-key factory, or <see langword="null" /> when a fixed key is used.</param>
    /// <exception cref="ArgumentException">Thrown when neither or both of the routing-key sources are supplied.</exception>
    protected RabbitMqRoutingKeyOutboundTarget(
        string name,
        IMessageSerializer serializer,
        IMessageContractRegistry messageContractRegistry,
        string topologyName,
        RabbitMqOutboundChannelGroup channelGroup,
        string exchangeName,
        bool isMandatory,
        string? constantRoutingKey,
        Func<TMessage, string>? routingKeyFactory
    )
        : base(name, serializer, messageContractRegistry, topologyName, channelGroup, exchangeName, isMandatory)
    {
        if (constantRoutingKey is null && routingKeyFactory is null)
        {
            throw new ArgumentException("A routing-key target must provide a constant key or a key factory.");
        }

        if (constantRoutingKey is not null && routingKeyFactory is not null)
        {
            throw new ArgumentException("A routing-key target cannot provide both a constant key and a key factory.");
        }

        _constantRoutingKey = constantRoutingKey;
        _routingKeyFactory = routingKeyFactory;
    }

    /// <inheritdoc />
    /// <exception cref="ArgumentException">Thrown when <paramref name="routingKey" /> is null or whitespace.</exception>
    /// <exception cref="CloudEventMetadataException">Thrown when <paramref name="message" /> does not implement <see cref="ICloudEvent" />.</exception>
    public Task PublishAsync(
        TMessage message,
        string routingKey,
        CancellationToken cancellationToken = default
    )
    {
        EnsureRoutingKey(routingKey);

        if (message is not ICloudEvent cloudEvent)
        {
            throw new CloudEventMetadataException(
                CloudEventAttributeNames.Id,
                "Implement ICloudEvent or derive from BaseCloudEvent, or call PublishAsync with explicit CloudEventMetadata."
            );
        }

        var metadata = CloudEventMetadata.From(cloudEvent);
        return PublishCoreAsync(message, metadata, type: null, dataSchema: null, routingKey, cancellationToken);
    }

    /// <inheritdoc />
    /// <exception cref="ArgumentException">Thrown when <paramref name="routingKey" /> is null or whitespace.</exception>
    public Task PublishAsync(
        TMessage message,
        in CloudEventMetadata metadata,
        string routingKey,
        CancellationToken cancellationToken = default
    )
    {
        EnsureRoutingKey(routingKey);
        return PublishCoreAsync(message, metadata, type: null, dataSchema: null, routingKey, cancellationToken);
    }

    /// <inheritdoc />
    /// <exception cref="ArgumentException">Thrown when <paramref name="routingKey" /> is null or whitespace.</exception>
    public Task PublishAsync(
        TMessage message,
        in CloudEventMetadata metadata,
        string type,
        string? dataSchema,
        string routingKey,
        CancellationToken cancellationToken = default
    )
    {
        EnsureRoutingKey(routingKey);
        return PublishCoreAsync(message, metadata, type, dataSchema, routingKey, cancellationToken);
    }

    /// <inheritdoc />
    protected override string GetRawRoutingKey()
    {
        return _constantRoutingKey ??
               throw new InvalidOperationException(
                   "Raw publishing is not supported for RabbitMQ outbound targets with message-derived routing keys."
               );
    }

    /// <inheritdoc />
    protected override string GetRoutingKey(TMessage message)
    {
        if (_constantRoutingKey is not null)
        {
            return _constantRoutingKey;
        }

        return _routingKeyFactory!(message) ??
               throw new InvalidOperationException("The RabbitMQ routing key factory returned null.");
    }

    private static void EnsureRoutingKey(string routingKey)
    {
        if (string.IsNullOrWhiteSpace(routingKey))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(routingKey));
        }
    }
}
