using System;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Usf.Transport.RabbitMq;

public sealed class DefaultRabbitMqChannelPool : IRabbitMqChannelPool
{
    private readonly Channel<PooledChannel> _availableChannels;
    private readonly TaskCompletionSource<object?> _allChannelsDisposed = new (
        TaskCreationOptions.RunContinuationsAsynchronously
    );
    private readonly Func<CancellationToken, Task<IChannel>> _channelFactory;
    private readonly int _maximumChannelCount;
    private int _disposed;
    private int _liveChannelCount;

    public DefaultRabbitMqChannelPool(int maximumChannelCount, Func<CancellationToken, Task<IChannel>> channelFactory)
    {
        if (maximumChannelCount < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maximumChannelCount),
                maximumChannelCount,
                "The value must be greater than zero."
            );
        }

        _maximumChannelCount = maximumChannelCount;
        _channelFactory = channelFactory ?? throw new ArgumentNullException(nameof(channelFactory));
        _availableChannels = Channel.CreateBounded<PooledChannel>(
            new BoundedChannelOptions(maximumChannelCount)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = false,
                SingleWriter = false
            }
        );
    }

    public async ValueTask<RabbitMqChannelLease> AcquireAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        while (true)
        {
            if (_availableChannels.Reader.TryRead(out var pooledChannel))
            {
                if (!pooledChannel.IsHealthy)
                {
                    await DiscardAsync(pooledChannel).ConfigureAwait(false);
                    continue;
                }

                return CreateLease(pooledChannel);
            }

            if (TryReserveChannelSlot())
            {
                try
                {
                    var channel = await _channelFactory(cancellationToken).ConfigureAwait(false);
                    var createdChannel = new PooledChannel(channel);

                    if (!createdChannel.IsHealthy)
                    {
                        await DiscardAsync(createdChannel).ConfigureAwait(false);
                        continue;
                    }

                    return CreateLease(createdChannel);
                }
                catch
                {
                    SignalChannelDisposed();
                    throw;
                }
            }

            try
            {
                pooledChannel = await _availableChannels.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (ChannelClosedException) when (Volatile.Read(ref _disposed) != 0)
            {
                throw new ObjectDisposedException(nameof(DefaultRabbitMqChannelPool));
            }

            if (!pooledChannel.IsHealthy)
            {
                await DiscardAsync(pooledChannel).ConfigureAwait(false);
                continue;
            }

            return CreateLease(pooledChannel);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
        {
            _availableChannels.Writer.TryComplete();

            while (_availableChannels.Reader.TryRead(out var pooledChannel))
            {
                await DiscardAsync(pooledChannel).ConfigureAwait(false);
            }

            if (Volatile.Read(ref _liveChannelCount) == 0)
            {
                _allChannelsDisposed.TrySetResult(null);
            }
        }

        await _allChannelsDisposed.Task.ConfigureAwait(false);
    }

    public void Dispose()
    {
        DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    public ValueTask ReleaseAsync(PooledChannel pooledChannel, long leaseId)
    {
        if (pooledChannel is null)
        {
            return default;
        }

        if (!pooledChannel.TryCompleteLease(leaseId))
        {
            return default;
        }

        if (Volatile.Read(ref _disposed) != 0 || !pooledChannel.IsHealthy || !_availableChannels.Writer.TryWrite(pooledChannel))
        {
            return DiscardAsync(pooledChannel);
        }

        return default;
    }

    private RabbitMqChannelLease CreateLease(PooledChannel pooledChannel)
    {
        var leaseId = pooledChannel.BeginLease();
        return new RabbitMqChannelLease(this, pooledChannel, leaseId);
    }

    private async ValueTask DiscardAsync(PooledChannel pooledChannel)
    {
        try
        {
            await pooledChannel.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            SignalChannelDisposed();
        }
    }

    private void SignalChannelDisposed()
    {
        if (Interlocked.Decrement(ref _liveChannelCount) == 0 && Volatile.Read(ref _disposed) != 0)
        {
            _allChannelsDisposed.TrySetResult(null);
        }
    }

    private bool TryReserveChannelSlot()
    {
        while (true)
        {
            ThrowIfDisposed();

            var current = Volatile.Read(ref _liveChannelCount);

            if (current >= _maximumChannelCount)
            {
                return false;
            }

            if (Interlocked.CompareExchange(ref _liveChannelCount, current + 1, current) == current)
            {
                return true;
            }
        }
    }

    private void ThrowIfDisposed()
    {
        if (Volatile.Read(ref _disposed) != 0)
        {
            throw new ObjectDisposedException(nameof(DefaultRabbitMqChannelPool));
        }
    }

    public sealed class PooledChannel : IAsyncDisposable
    {
        private readonly IChannel _channel;
        private long _currentLeaseId;
        private int _observedShutdown;
        private long _nextLeaseId;

        public PooledChannel(IChannel channel)
        {
            _channel = channel ?? throw new ArgumentNullException(nameof(channel));
            _channel.ChannelShutdownAsync += OnChannelShutdownAsync;
        }

        public IChannel Channel => _channel;

        public bool IsHealthy => Volatile.Read(ref _observedShutdown) == 0 && _channel.IsOpen;

        public long BeginLease()
        {
            var leaseId = Interlocked.Increment(ref _nextLeaseId);

            if (Interlocked.CompareExchange(ref _currentLeaseId, leaseId, 0) != 0)
            {
                throw new InvalidOperationException("A RabbitMQ channel cannot be leased concurrently.");
            }

            return leaseId;
        }

        public bool TryCompleteLease(long leaseId)
        {
            return Interlocked.CompareExchange(ref _currentLeaseId, 0, leaseId) == leaseId;
        }

        public async ValueTask DisposeAsync()
        {
            _channel.ChannelShutdownAsync -= OnChannelShutdownAsync;

            if (_channel is IAsyncDisposable asyncDisposable)
            {
                await asyncDisposable.DisposeAsync().ConfigureAwait(false);
                return;
            }

            _channel.Dispose();
        }

        private Task OnChannelShutdownAsync(object sender, ShutdownEventArgs eventArgs)
        {
            Interlocked.Exchange(ref _observedShutdown, 1);
            return Task.CompletedTask;
        }
    }
}
