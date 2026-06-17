using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using Bmf.Core.Messaging;

namespace Bmf.Transport.RabbitMq;

/// <summary>
/// Opens channels on a topology's connection and validates, once, that the topology's worst-case channel count
/// fits within the broker's negotiated channel maximum.
/// </summary>
public sealed class RabbitMqChannelSource : IDisposable
{
    private readonly SemaphoreSlim _channelBudgetValidationGate = new (1, 1);
    private readonly RabbitMqConnectionProvider _connectionProvider;
    private int _channelBudgetConfigured;
    private int _channelBudgetValidated;
    private int _worstCaseChannelCount;
    private string _worstCaseChannelCountDescription = string.Empty;

    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMqChannelSource" /> class.
    /// </summary>
    /// <param name="connectionProvider">The provider that supplies the underlying connection.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="connectionProvider" /> is <see langword="null" />.</exception>
    public RabbitMqChannelSource(RabbitMqConnectionProvider connectionProvider)
    {
        _connectionProvider = connectionProvider ?? throw new ArgumentNullException(nameof(connectionProvider));
    }

    /// <summary>
    /// Disposes the channel source.
    /// </summary>
    public void Dispose()
    {
        _channelBudgetValidationGate.Dispose();
    }

    /// <summary>
    /// Configures the worst-case channel budget that is validated against the broker's negotiated channel
    /// maximum on first connection. May only be called once.
    /// </summary>
    /// <param name="worstCaseChannelCount">The maximum number of channels the topology may open concurrently.</param>
    /// <param name="worstCaseChannelCountDescription">A human-readable explanation of how the count is derived, used in the validation error.</param>
    /// <exception cref="InvalidOperationException">Thrown when the budget has already been configured.</exception>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="worstCaseChannelCountDescription" /> is <see langword="null" />.</exception>
    public void SetChannelBudget(int worstCaseChannelCount, string worstCaseChannelCountDescription)
    {
        if (Interlocked.Exchange(ref _channelBudgetConfigured, 1) != 0)
        {
            throw new InvalidOperationException("The RabbitMQ channel budget can only be configured once.");
        }

        _worstCaseChannelCount = worstCaseChannelCount;
        _worstCaseChannelCountDescription =
            worstCaseChannelCountDescription ??
            throw new ArgumentNullException(nameof(worstCaseChannelCountDescription));
    }

    /// <summary>
    /// Opens a new channel on the connection using default options.
    /// </summary>
    /// <param name="cancellationToken">A token to observe while opening the channel.</param>
    /// <returns>The opened channel.</returns>
    public async Task<IChannel> CreateChannelAsync(CancellationToken cancellationToken = default)
    {
        var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        return await connection.CreateChannelAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Opens a new channel on the connection using the given options.
    /// </summary>
    /// <param name="options">The channel options, or <see langword="null" /> for defaults.</param>
    /// <param name="cancellationToken">A token to observe while opening the channel.</param>
    /// <returns>The opened channel.</returns>
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

    /// <summary>
    /// Gets the underlying connection, validating the channel budget on the first call.
    /// </summary>
    /// <param name="cancellationToken">A token to observe while obtaining the connection.</param>
    /// <returns>The connection.</returns>
    /// <exception cref="TopologyValidationException">Thrown when the worst-case channel count exceeds the broker's negotiated channel maximum.</exception>
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

        throw new TopologyValidationException(
            new List<string>
            {
                $"RabbitMQ topology may open up to {_worstCaseChannelCount} channels ({_worstCaseChannelCountDescription}), but the broker negotiated channel_max={connection.ChannelMax}."
            }
        );
    }
}
