using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Usf.Transport.RabbitMq;

public sealed class RabbitMqConnectionProvider : IAsyncDisposable, IDisposable
{
    private readonly Func<CancellationToken, Task<IConnection>> _createConnectionAsync;

    private readonly ILogger _logger;

    // The SemaphoreSlim is intentionally not disposed of in this class. SemaphoreSlim only needs to be disposed of
    // when its AvailableWaitHandle property is accessed - this creates a ManuelResetEvent under the covers which
    // has a finalizer and needs to be disposed of. We don't do this here, the provider is a sealed class, and the
    // semaphore is private readonly. This allows us to use the semaphore in subsequent calls to Dispose(Async) without
    // introducing a more complex disposal mechanism (like a state machine) that disposes the semaphore, too.
    // See https://stackoverflow.com/questions/32033416/do-i-need-to-dispose-a-semaphoreslim
    private readonly SemaphoreSlim _semaphore = new (1, 1);
    private Task<IConnection>? _connectionTask;
    private bool _disposed;

    public RabbitMqConnectionProvider(
        Func<CancellationToken, Task<IConnection>> createConnectionAsync,
        ILogger? logger = null
    )
    {
        _createConnectionAsync =
            createConnectionAsync ?? throw new ArgumentNullException(nameof(createConnectionAsync));
        _logger = logger ?? NullLogger.Instance;
    }

    public async ValueTask DisposeAsync()
    {
        await _semaphore.WaitAsync(CancellationToken.None).ConfigureAwait(false);

        try
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;

            if (_connectionTask is not null && _connectionTask.Status == TaskStatus.RanToCompletion)
            {
                var connection = await _connectionTask.ConfigureAwait(false);
                Unsubscribe(connection);

                if (connection is IAsyncDisposable asyncDisposable)
                {
                    await asyncDisposable.DisposeAsync().ConfigureAwait(false);
                }
                else
                {
                    connection.Dispose();
                }
            }
        }
        finally
        {
            _semaphore.Release();
        }
    }

    public void Dispose()
    {
        _semaphore.Wait();

        try
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;

            if (_connectionTask is not null && _connectionTask.Status == TaskStatus.RanToCompletion)
            {
                var connection = _connectionTask.Result;
                Unsubscribe(connection);
                connection.Dispose();
            }
        }
        finally
        {
            _semaphore.Release();
        }
    }

    public async Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken)
    {
        await _semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            ThrowIfDisposed();

            _connectionTask ??= CreateConnectionAsync(cancellationToken);

            try
            {
                return await _connectionTask.ConfigureAwait(false);
            }
            catch
            {
                // A failed creation attempt must not poison the provider: clear the cached task so the
                // next acquisition retries instead of replaying the same faulted task forever.
                _connectionTask = null;
                throw;
            }
        }
        finally
        {
            _semaphore.Release();
        }
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(RabbitMqConnectionProvider));
        }
    }

    private async Task<IConnection> CreateConnectionAsync(CancellationToken cancellationToken)
    {
        var connection = await _createConnectionAsync(cancellationToken).ConfigureAwait(false);
        Subscribe(connection);
        return connection;
    }

    private Task OnConnectionRecoveryErrorAsync(object sender, ConnectionRecoveryErrorEventArgs eventArgs)
    {
        _logger.LogWarning(
            eventArgs.Exception,
            "RabbitMQ connection lifecycle transition {Transition}",
            "recovery-failed"
        );
        return Task.CompletedTask;
    }

    private Task OnConnectionShutdownAsync(object sender, ShutdownEventArgs eventArgs)
    {
        _logger.LogWarning(
            "RabbitMQ connection lifecycle transition {Transition}: initiator {Initiator}, reply code {ReplyCode}, reply text {ReplyText}",
            "shutdown",
            eventArgs.Initiator,
            eventArgs.ReplyCode,
            eventArgs.ReplyText
        );
        return Task.CompletedTask;
    }

    private Task OnRecoverySucceededAsync(object sender, AsyncEventArgs eventArgs)
    {
        _logger.LogInformation(
            "RabbitMQ connection lifecycle transition {Transition}",
            "recovery-succeeded"
        );
        return Task.CompletedTask;
    }

    private void Subscribe(IConnection connection)
    {
        connection.ConnectionShutdownAsync += OnConnectionShutdownAsync;
        connection.RecoverySucceededAsync += OnRecoverySucceededAsync;
        connection.ConnectionRecoveryErrorAsync += OnConnectionRecoveryErrorAsync;
    }

    private void Unsubscribe(IConnection connection)
    {
        connection.ConnectionShutdownAsync -= OnConnectionShutdownAsync;
        connection.RecoverySucceededAsync -= OnRecoverySucceededAsync;
        connection.ConnectionRecoveryErrorAsync -= OnConnectionRecoveryErrorAsync;
    }
}
