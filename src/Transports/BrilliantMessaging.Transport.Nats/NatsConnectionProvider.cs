using System;
using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging;
using NATS.Client.Core;
using NATS.Client.JetStream;

namespace BrilliantMessaging.Transport.Nats;

/// <summary>
/// Lazily owns a NATS connection and JetStream context for a topology.
/// </summary>
public sealed class NatsConnectionProvider : IAsyncDisposable
{
    private readonly Func<CancellationToken, Task<NatsOpts>> _createOptions;
    private readonly SemaphoreSlim _sync = new (1, 1);
    private NatsConnection? _connection;
    private bool _disposed;
    private NatsJSContext? _jetStream;

    /// <summary>
    /// Initializes a new instance of the <see cref="NatsConnectionProvider" /> class.
    /// </summary>
    public NatsConnectionProvider(Func<CancellationToken, Task<NatsOpts>> createOptions)
    {
        _createOptions = createOptions ?? throw new ArgumentNullException(nameof(createOptions));
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await _sync.WaitAsync(CancellationToken.None).ConfigureAwait(false);
        try
        {
            if (_disposed)
            {
                return;
            }

            Volatile.Write(ref _disposed, true);
            if (_connection is not null)
            {
                await _connection.DisposeAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            _sync.Release();
        }
    }

    /// <summary>
    /// Gets the topology's JetStream context.
    /// </summary>
    public async Task<NatsJSContext> GetJetStreamAsync(CancellationToken cancellationToken = default)
    {
        // The fast path reads both fields outside the semaphore that publishes them, so it needs the
        // matching acquire fence: without it the context reference can become visible before the writes
        // its constructor performed, handing a caller a partially initialized object.
        if (Volatile.Read(ref _disposed))
        {
            throw new ObjectDisposedException(nameof(NatsConnectionProvider));
        }

        if (Volatile.Read(ref _jetStream) is { } cached)
        {
            return cached;
        }

        await _sync.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(NatsConnectionProvider));
            }

            if (_jetStream is not null)
            {
                return _jetStream;
            }

            var options = await _createOptions(cancellationToken).ConfigureAwait(false);
            if (options is null)
            {
                throw new TopologyValidationException(["A NATS options factory returned null."]);
            }

            NatsConnection connection = new (options);
            try
            {
                await connection.ConnectAsync().ConfigureAwait(false);
            }
            catch
            {
                await connection.DisposeAsync().ConfigureAwait(false);
                throw;
            }

            _connection = connection;
            NatsJSContext jetStream = new (connection);
            Volatile.Write(ref _jetStream, jetStream);
            return jetStream;
        }
        finally
        {
            _sync.Release();
        }
    }
}
