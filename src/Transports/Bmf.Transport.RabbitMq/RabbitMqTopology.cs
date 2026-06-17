using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using Bmf.Core.Messaging;
using Bmf.Core.Messaging.Inbound;
using Bmf.Core.Messaging.Outbound;

using Bmf.Transport.RabbitMq.Inbound;
using Bmf.Transport.RabbitMq.Outbound;

namespace Bmf.Transport.RabbitMq;

/// <summary>
/// The compiled RabbitMQ topology. It extends the Core <see cref="Topology" /> with RabbitMQ-specific runtime
/// state: exchanges, queues, bindings, outbound channel groups, inbound
/// channel groups, outbound targets, inbound endpoints, the inbound pipeline, the shutdown timeout, the
/// connection provider, and the channel source. A topology owns exactly one
/// <see cref="RabbitMqConnectionProvider" />; register separate topology instances when separate publisher and
/// consumer connections are wanted, preferably via
/// <see cref="RabbitMqTransportModule.AddRabbitMqOutboundTopology(BmfBuilder, System.Action{Bmf.Transport.RabbitMq.Outbound.IRabbitMqOutboundTopologyBuilder})" />
/// and <see cref="RabbitMqTransportModule.AddRabbitMqInboundTopology(BmfBuilder, System.Action{Bmf.Transport.RabbitMq.Inbound.IRabbitMqInboundTopologyBuilder})" />.
/// </summary>
public sealed class RabbitMqTopology : Topology, IAsyncDisposable, IDisposable
{
    /// <summary>
    /// The default name used by
    /// <see cref="RabbitMqTransportModule.AddRabbitMqInboundTopology(BmfBuilder, Action{IRabbitMqInboundTopologyBuilder})" />.
    /// It deliberately differs from <see cref="Topology.DefaultName" /> so that an outbound topology and an
    /// inbound topology registered without explicit names do not collide: publish call sites resolve the
    /// default topology by <see cref="Topology.DefaultName" />, while inbound topologies are only started via
    /// <see cref="ITopologyRuntime" /> and their name is purely a catalog and diagnostics identity.
    /// </summary>
    public const string DefaultInboundName = "default-inbound";

    private readonly RabbitMqChannelSource _channelSource;
    private readonly RabbitMqConnectionProvider _connectionProvider;
    private readonly IReadOnlyDictionary<InboundEndpointSelectionKey, RabbitMqInboundEndpoint> _dispatchIndex;
    private int _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMqTopology" /> class. This constructor is invoked by
    /// the topology compiler with the fully resolved runtime state and is not intended to be called directly.
    /// </summary>
    /// <param name="name">The topology name.</param>
    /// <param name="data">The compiled Core topology data (targets and endpoints).</param>
    /// <param name="messageContractRegistry">The effective message-contract registry for the topology.</param>
    /// <param name="exchanges">The exchange declarations.</param>
    /// <param name="queues">The queue declarations.</param>
    /// <param name="bindings">The binding declarations.</param>
    /// <param name="outboundChannelGroups">The outbound channel groups.</param>
    /// <param name="targets">The outbound targets.</param>
    /// <param name="inboundChannelGroups">The inbound channel groups.</param>
    /// <param name="consumers">The inbound consumers.</param>
    /// <param name="endpoints">The inbound endpoints.</param>
    /// <param name="dispatchIndex">The index mapping queue/discriminator pairs to inbound endpoints.</param>
    /// <param name="pipeline">The composed inbound message pipeline.</param>
    /// <param name="shutdownTimeout">The graceful shutdown timeout.</param>
    /// <param name="connectionProvider">The connection provider that owns the topology's single connection.</param>
    /// <param name="channelSource">The channel source used to open channels.</param>
    /// <exception cref="ArgumentNullException">Thrown when any reference argument is <see langword="null" />.</exception>
    public RabbitMqTopology(
        string name,
        TopologyData data,
        IMessageContractRegistry messageContractRegistry,
        IReadOnlyList<RabbitMqExchangeDefinition> exchanges,
        IReadOnlyList<RabbitMqQueueDefinition> queues,
        IReadOnlyList<RabbitMqBindingDefinition> bindings,
        IReadOnlyList<RabbitMqOutboundChannelGroup> outboundChannelGroups,
        IReadOnlyList<OutboundTarget> targets,
        IReadOnlyList<RabbitMqInboundChannelGroup> inboundChannelGroups,
        IReadOnlyList<RabbitMqInboundConsumer> consumers,
        IReadOnlyList<RabbitMqInboundEndpoint> endpoints,
        IReadOnlyDictionary<InboundEndpointSelectionKey, RabbitMqInboundEndpoint> dispatchIndex,
        MessageDelegate pipeline,
        TimeSpan shutdownTimeout,
        RabbitMqConnectionProvider connectionProvider,
        RabbitMqChannelSource channelSource
    ) : base(name, data)
    {
        MessageContractRegistry = messageContractRegistry ??
                                  throw new ArgumentNullException(nameof(messageContractRegistry));
        Exchanges = exchanges ?? throw new ArgumentNullException(nameof(exchanges));
        Queues = queues ?? throw new ArgumentNullException(nameof(queues));
        Bindings = bindings ?? throw new ArgumentNullException(nameof(bindings));
        OutboundChannelGroups = outboundChannelGroups ?? throw new ArgumentNullException(nameof(outboundChannelGroups));
        Targets = targets ?? throw new ArgumentNullException(nameof(targets));
        InboundChannelGroups = inboundChannelGroups ?? throw new ArgumentNullException(nameof(inboundChannelGroups));
        Consumers = consumers ?? throw new ArgumentNullException(nameof(consumers));
        Endpoints = endpoints ?? throw new ArgumentNullException(nameof(endpoints));
        _dispatchIndex = dispatchIndex ?? throw new ArgumentNullException(nameof(dispatchIndex));
        Pipeline = pipeline ?? throw new ArgumentNullException(nameof(pipeline));
        ShutdownTimeout = shutdownTimeout;
        _connectionProvider = connectionProvider ?? throw new ArgumentNullException(nameof(connectionProvider));
        _channelSource = channelSource ?? throw new ArgumentNullException(nameof(channelSource));
    }

    /// <summary>
    /// Gets the effective message-contract registry for the topology.
    /// </summary>
    public IMessageContractRegistry MessageContractRegistry { get; }

    /// <summary>
    /// Gets the exchange declarations.
    /// </summary>
    public IReadOnlyList<RabbitMqExchangeDefinition> Exchanges { get; }

    /// <summary>
    /// Gets the queue declarations.
    /// </summary>
    public IReadOnlyList<RabbitMqQueueDefinition> Queues { get; }

    /// <summary>
    /// Gets the binding declarations.
    /// </summary>
    public IReadOnlyList<RabbitMqBindingDefinition> Bindings { get; }

    /// <summary>
    /// Gets the outbound channel groups.
    /// </summary>
    public IReadOnlyList<RabbitMqOutboundChannelGroup> OutboundChannelGroups { get; }

    /// <summary>
    /// Gets the outbound targets.
    /// </summary>
    public IReadOnlyList<OutboundTarget> Targets { get; }

    /// <summary>
    /// Gets the inbound channel groups.
    /// </summary>
    public IReadOnlyList<RabbitMqInboundChannelGroup> InboundChannelGroups { get; }

    /// <summary>
    /// Gets the inbound consumers.
    /// </summary>
    public IReadOnlyList<RabbitMqInboundConsumer> Consumers { get; }

    /// <summary>
    /// Gets the inbound endpoints.
    /// </summary>
    public IReadOnlyList<RabbitMqInboundEndpoint> Endpoints { get; }

    /// <summary>
    /// Gets the composed inbound message pipeline.
    /// </summary>
    public MessageDelegate Pipeline { get; }

    /// <summary>
    /// Gets the graceful shutdown timeout for the topology runtime.
    /// </summary>
    public TimeSpan ShutdownTimeout { get; }

    /// <summary>
    /// Gets the consumers grouped by their inbound channel group.
    /// </summary>
    public IEnumerable<IGrouping<RabbitMqInboundChannelGroup, RabbitMqInboundConsumer>> ConsumersByChannelGroup =>
        Consumers.GroupBy(static consumer => consumer.ChannelGroup);

    /// <summary>
    /// Asynchronously disposes the topology, disposing its outbound channel groups, channel source, and
    /// connection provider. Disposal is idempotent.
    /// </summary>
    /// <returns>A task that completes when disposal has finished.</returns>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        foreach (var channelGroup in OutboundChannelGroups)
        {
            await channelGroup.DisposeAsync().ConfigureAwait(false);
        }

        _channelSource.Dispose();
        await _connectionProvider.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Disposes the topology, disposing its outbound channel groups, channel source, and connection provider.
    /// Disposal is idempotent.
    /// </summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        foreach (var channelGroup in OutboundChannelGroups)
        {
            channelGroup.Dispose();
        }

        _channelSource.Dispose();
        _connectionProvider.Dispose();
    }

    /// <summary>
    /// Opens a new channel on the topology's connection using default options.
    /// </summary>
    /// <param name="cancellationToken">A token to observe while opening the channel.</param>
    /// <returns>The opened channel.</returns>
    public Task<IChannel> CreateChannelAsync(CancellationToken cancellationToken = default)
    {
        return _channelSource.CreateChannelAsync(cancellationToken);
    }

    /// <summary>
    /// Opens a new channel on the topology's connection using the given options.
    /// </summary>
    /// <param name="options">The channel options, or <see langword="null" /> for defaults.</param>
    /// <param name="cancellationToken">A token to observe while opening the channel.</param>
    /// <returns>The opened channel.</returns>
    public Task<IChannel> CreateChannelAsync(
        CreateChannelOptions? options,
        CancellationToken cancellationToken = default
    )
    {
        return _channelSource.CreateChannelAsync(options, cancellationToken);
    }

    /// <summary>
    /// Gets the topology's underlying connection, opening it if necessary.
    /// </summary>
    /// <param name="cancellationToken">A token to observe while obtaining the connection.</param>
    /// <returns>The connection.</returns>
    public Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
    {
        return _channelSource.GetConnectionAsync(cancellationToken);
    }

    /// <summary>
    /// Attempts to resolve the inbound endpoint for a queue and CloudEvents discriminator.
    /// </summary>
    /// <param name="queueName">The queue the message arrived on.</param>
    /// <param name="discriminator">The CloudEvents discriminator of the message.</param>
    /// <param name="endpoint">When this method returns, the matching endpoint, or <see langword="null" /> when none was found.</param>
    /// <returns><see langword="true" /> when an endpoint was found; otherwise <see langword="false" />.</returns>
    public bool TryGetEndpoint(
        string queueName,
        string discriminator,
        [NotNullWhen(true)] out RabbitMqInboundEndpoint? endpoint
    )
    {
        return _dispatchIndex.TryGetValue(new InboundEndpointSelectionKey(queueName, discriminator), out endpoint);
    }
}
