using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Usf.Core.Messaging;

namespace Usf.Core.Tests.Messaging.TestSupport;

/// <summary>
/// A recording outbound target that intentionally does not implement
/// <see cref="IOutboundRoutableTarget{T}" />, so callers that supply a routing key are rejected.
/// </summary>
public sealed class NonRoutableRecordingTarget<TMessage> : OutboundTarget<TMessage>
{
    public NonRoutableRecordingTarget(
        string name,
        IMessageSerializer serializer,
        string? topologyName = null
    )
        : base(name, "test", serializer, CloudEventsTestFactory.CreateRegistry(), topologyName) { }

    public List<TMessage> Messages { get; } = [];

    protected override Task PublishSerializedCoreAsync(
        SerializedMessage message,
        CancellationToken cancellationToken
    )
    {
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
        return Task.CompletedTask;
    }
}
