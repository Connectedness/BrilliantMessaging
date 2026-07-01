using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;
using BrilliantMessaging.Core.Messaging.Outbound;
using BrilliantMessaging.Transport.Nats.Inbound;
using NATS.Client.JetStream;

namespace BrilliantMessaging.Transport.Nats;

/// <summary>
/// A compiled NATS JetStream topology.
/// </summary>
public sealed class NatsTopology : Topology, IAsyncDisposable
{
    /// <summary>
    /// The transport name used for diagnostics and transport messages.
    /// </summary>
    public const string TransportNameValue = "nats";

    /// <summary>
    /// The default name for parameterless NATS inbound topology registration.
    /// </summary>
    public const string DefaultInboundName = "default-nats-inbound";

    private readonly NatsConnectionProvider _connectionProvider;
    private readonly IReadOnlyDictionary<InboundEndpointSelectionKey, NatsInboundEndpoint> _dispatchIndex;
    private int _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="NatsTopology" /> class.
    /// </summary>
    public NatsTopology(
        string name,
        TopologyData data,
        IMessageContractRegistry messageContractRegistry,
        IReadOnlyList<NatsStreamDefinition> streams,
        IReadOnlyList<OutboundTarget> targets,
        IReadOnlyList<NatsInboundConsumer> consumers,
        IReadOnlyList<NatsInboundEndpoint> endpoints,
        IReadOnlyDictionary<InboundEndpointSelectionKey, NatsInboundEndpoint> dispatchIndex,
        MessageDelegate pipeline,
        TimeSpan shutdownTimeout,
        NatsTopologyProvisioningMode provisioningMode,
        bool ackProgressEnabled,
        NatsConnectionProvider connectionProvider
    ) : base(name, data)
    {
        MessageContractRegistry = messageContractRegistry ??
                                  throw new ArgumentNullException(nameof(messageContractRegistry));
        Streams = streams ?? throw new ArgumentNullException(nameof(streams));
        Targets = targets ?? throw new ArgumentNullException(nameof(targets));
        Consumers = consumers ?? throw new ArgumentNullException(nameof(consumers));
        Endpoints = endpoints ?? throw new ArgumentNullException(nameof(endpoints));
        _dispatchIndex = dispatchIndex ?? throw new ArgumentNullException(nameof(dispatchIndex));
        Pipeline = pipeline ?? throw new ArgumentNullException(nameof(pipeline));
        ShutdownTimeout = shutdownTimeout;
        ProvisioningMode = provisioningMode;
        AckProgressEnabled = ackProgressEnabled;
        _connectionProvider = connectionProvider ?? throw new ArgumentNullException(nameof(connectionProvider));
    }

    /// <summary>
    /// Gets the effective message contract registry.
    /// </summary>
    public IMessageContractRegistry MessageContractRegistry { get; }

    /// <summary>
    /// Gets the declared streams.
    /// </summary>
    public IReadOnlyList<NatsStreamDefinition> Streams { get; }

    /// <summary>
    /// Gets the compiled outbound targets.
    /// </summary>
    public IReadOnlyList<OutboundTarget> Targets { get; }

    /// <summary>
    /// Gets the compiled durable consumers.
    /// </summary>
    public IReadOnlyList<NatsInboundConsumer> Consumers { get; }

    /// <summary>
    /// Gets the compiled inbound endpoints.
    /// </summary>
    public IReadOnlyList<NatsInboundEndpoint> Endpoints { get; }

    /// <summary>
    /// Gets the inbound pipeline.
    /// </summary>
    public MessageDelegate Pipeline { get; }

    /// <summary>
    /// Gets the graceful shutdown timeout.
    /// </summary>
    public TimeSpan ShutdownTimeout { get; }

    /// <summary>
    /// Gets the provisioning mode.
    /// </summary>
    public NatsTopologyProvisioningMode ProvisioningMode { get; }

    /// <summary>
    /// Gets a value indicating whether AckProgress heartbeats are enabled.
    /// </summary>
    public bool AckProgressEnabled { get; }

    /// <summary>
    /// Gets the consumers grouped by stream.
    /// </summary>
    public IEnumerable<IGrouping<string, NatsInboundConsumer>> ConsumersByStream =>
        Consumers.GroupBy(static consumer => consumer.StreamName, StringComparer.Ordinal);

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        await _connectionProvider.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Gets the topology's JetStream context.
    /// </summary>
    public Task<NatsJSContext> GetJetStreamAsync(
        CancellationToken cancellationToken = default
    )
    {
        return _connectionProvider.GetJetStreamAsync(cancellationToken);
    }

    /// <summary>
    /// Finds an endpoint for the transport source and CloudEvents discriminator.
    /// For NATS, the source is the consumer filter subject when one is configured, otherwise the durable name.
    /// </summary>
    public bool TryGetEndpoint(string source, string discriminator, out NatsInboundEndpoint endpoint)
    {
        return _dispatchIndex.TryGetValue(new InboundEndpointSelectionKey(source, discriminator), out endpoint!);
    }
}
