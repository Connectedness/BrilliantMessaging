using System;
using System.Threading;
using System.Threading.Tasks;
using Usf.Abstractions;

namespace Usf.Core.Messaging;

public readonly struct TopologyPublisher
{
    private readonly TopologyName _topologyName;

    public TopologyPublisher(MessagePublisher router, TopologyName topologyName)
    {
        Router = router ?? throw new ArgumentNullException(nameof(router));
        _topologyName = topologyName;
    }

    public Task PublishMessageAsync<T>(
        T message,
        OutboundTarget? target = null,
        CancellationToken cancellationToken = default
    ) where T : ICloudEvent
    {
        return Router.PublishMessageAsync(message, target, _topologyName, cancellationToken);
    }

    public Task PublishMessageAsync<T>(
        T message,
        in CloudEventMetadata metadata,
        OutboundTarget? target = null,
        CancellationToken cancellationToken = default
    )
    {
        return Router.PublishMessageAsync(message, in metadata, target, _topologyName, cancellationToken);
    }

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
