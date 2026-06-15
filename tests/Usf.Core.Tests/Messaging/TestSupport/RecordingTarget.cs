using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Usf.Abstractions;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Errors;

namespace Usf.Core.Tests.Messaging.TestSupport;

public sealed class RecordingTarget<TMessage> : OutboundTarget<TMessage>, IOutboundRoutableTarget<TMessage>
{
    public RecordingTarget(string name, IMessageSerializer serializer)
        : this(name, serializer, CloudEventsTestFactory.CreateRegistry()) { }

    public RecordingTarget(
        string name,
        IMessageSerializer serializer,
        IMessageContractRegistry messageContractRegistry,
        string? topologyName = null
    )
        : base(name, "test", serializer, messageContractRegistry, topologyName) { }

    public List<TMessage> Messages { get; } = [];

    public List<CloudEventEnvelope> CloudEventEnvelopes { get; } = [];

    public List<string?> RoutingKeys { get; } = [];

    public List<SerializedMessage> SerializedMessages { get; } = [];

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

    protected override Task PublishSerializedCoreAsync(
        SerializedMessage message,
        CancellationToken cancellationToken
    )
    {
        SerializedMessages.Add(message);
        return Task.CompletedTask;
    }

    protected override Task PublishTypedCloudEventAsync(
        TMessage message,
        CloudEventEnvelope envelope,
        string? routingKey,
        CancellationToken cancellationToken
    )
    {
        Messages.Add(message);
        CloudEventEnvelopes.Add(envelope);
        RoutingKeys.Add(routingKey);
        return Task.CompletedTask;
    }

    private static void EnsureRoutingKey(string routingKey)
    {
        if (string.IsNullOrWhiteSpace(routingKey))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(routingKey));
        }
    }
}
