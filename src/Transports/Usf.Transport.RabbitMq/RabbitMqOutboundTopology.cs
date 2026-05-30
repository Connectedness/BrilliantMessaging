using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Errors;
using Usf.Transport.RabbitMq.Configuration;

namespace Usf.Transport.RabbitMq;

public sealed class RabbitMqOutboundTopology : IAsyncDisposable, IDisposable
{
    private readonly SemaphoreSlim _channelBudgetValidationGate = new (1, 1);
    private readonly RabbitMqConnectionProvider _connectionProvider;
    private readonly int _worstCaseChannelCount;
    private readonly string _worstCaseChannelCountDescription;
    private int _channelBudgetValidated;
    private int _disposed;

    public RabbitMqOutboundTopology(
        OutboundTopology outboundTopology,
        IReadOnlyList<RabbitMqExchangeDefinition> exchanges,
        IReadOnlyList<RabbitMqQueueDefinition> queues,
        IReadOnlyList<RabbitMqBindingDefinition> bindings,
        IReadOnlyList<RabbitMqAddressDefinition> addresses,
        IReadOnlyList<RabbitMqChannelGroup> channelGroups,
        IReadOnlyList<OutboundTarget> targets,
        RabbitMqConnectionProvider connectionProvider,
        int worstCaseChannelCount,
        string worstCaseChannelCountDescription
    )
    {
        OutboundTopology = outboundTopology ?? throw new ArgumentNullException(nameof(outboundTopology));
        Exchanges = exchanges ?? throw new ArgumentNullException(nameof(exchanges));
        Queues = queues ?? throw new ArgumentNullException(nameof(queues));
        Bindings = bindings ?? throw new ArgumentNullException(nameof(bindings));
        Addresses = addresses ?? throw new ArgumentNullException(nameof(addresses));
        ChannelGroups = channelGroups ?? throw new ArgumentNullException(nameof(channelGroups));
        Targets = targets ?? throw new ArgumentNullException(nameof(targets));
        _connectionProvider = connectionProvider ?? throw new ArgumentNullException(nameof(connectionProvider));
        _worstCaseChannelCount = worstCaseChannelCount;
        _worstCaseChannelCountDescription = worstCaseChannelCountDescription ??
                                            throw new ArgumentNullException(
                                                nameof(worstCaseChannelCountDescription)
                                            );
    }

    public OutboundTopology OutboundTopology { get; }

    public IReadOnlyList<RabbitMqExchangeDefinition> Exchanges { get; }

    public IReadOnlyList<RabbitMqQueueDefinition> Queues { get; }

    public IReadOnlyList<RabbitMqBindingDefinition> Bindings { get; }

    public IReadOnlyList<RabbitMqAddressDefinition> Addresses { get; }

    public IReadOnlyList<RabbitMqChannelGroup> ChannelGroups { get; }

    public IReadOnlyList<OutboundTarget> Targets { get; }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        foreach (var channelGroup in ChannelGroups)
        {
            await channelGroup.DisposeAsync().ConfigureAwait(false);
        }

        _channelBudgetValidationGate.Dispose();
        await _connectionProvider.DisposeAsync().ConfigureAwait(false);
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        foreach (var channelGroup in ChannelGroups)
        {
            channelGroup.Dispose();
        }

        _channelBudgetValidationGate.Dispose();
        _connectionProvider.Dispose();
    }

    public async Task<IChannel> CreateChannelAsync(CancellationToken cancellationToken = default)
    {
        var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        return await connection.CreateChannelAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    public async Task<IChannel> CreateChannelAsync(
        CreateChannelOptions? options,
        CancellationToken cancellationToken = default
    )
    {
        if (options is null)
        {
            return await CreateChannelAsync(cancellationToken).ConfigureAwait(false);
        }

        var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        return await connection.CreateChannelAsync(options, cancellationToken).ConfigureAwait(false);
    }

    public async Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
    {
        var connection = await _connectionProvider.GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        await ValidateChannelBudgetOnceAsync(connection, cancellationToken).ConfigureAwait(false);
        return connection;
    }

    private async Task ValidateChannelBudgetOnceAsync(IConnection connection, CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref _channelBudgetValidated) != 0)
        {
            return;
        }

        await _channelBudgetValidationGate.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            if (Volatile.Read(ref _channelBudgetValidated) != 0)
            {
                return;
            }

            ValidateChannelBudget(connection);
            Volatile.Write(ref _channelBudgetValidated, 1);
        }
        finally
        {
            _channelBudgetValidationGate.Release();
        }
    }

    private void ValidateChannelBudget(IConnection connection)
    {
        if (_worstCaseChannelCount == 0 || connection.ChannelMax == 0)
        {
            return;
        }

        if (_worstCaseChannelCount <= connection.ChannelMax)
        {
            return;
        }

        throw new OutboundTopologyValidationException(
            new List<string>
            {
                $"RabbitMQ outbound topology may open up to {_worstCaseChannelCount} channels ({_worstCaseChannelCountDescription}), but the broker negotiated channel_max={connection.ChannelMax}."
            }
        );
    }
}
