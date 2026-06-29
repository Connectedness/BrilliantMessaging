using System;
using System.Threading;
using System.Threading.Tasks;

namespace BrilliantMessaging.Transport.InMemory;

/// <summary>
/// The default <see cref="IInMemoryDelayScheduler" />, which waits real wall-clock time through
/// <see cref="Task.Delay(TimeSpan, CancellationToken)" />.
/// </summary>
public sealed class RealTimeInMemoryDelayScheduler : IInMemoryDelayScheduler
{
    /// <inheritdoc />
    public Task DelayAsync(TimeSpan delay, CancellationToken cancellationToken = default)
    {
        if (delay <= TimeSpan.Zero)
        {
            return cancellationToken.IsCancellationRequested ?
                Task.FromCanceled(cancellationToken) :
                Task.CompletedTask;
        }

        return Task.Delay(delay, cancellationToken);
    }
}
