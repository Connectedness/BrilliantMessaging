using System;
using System.Threading;
using System.Threading.Tasks;

namespace BrilliantMessaging.Transport.InMemory;

/// <summary>
/// Abstracts the delay used to schedule a retry delivery so automated tests can drive delayed retries
/// deterministically instead of sleeping. The default runtime registration uses real wall-clock time; tests
/// supply a hand-crafted scheduler that completes pending delays on demand.
/// </summary>
public interface IInMemoryDelayScheduler
{
    /// <summary>
    /// Returns a task that completes after the given delay has elapsed, or faults with
    /// <see cref="OperationCanceledException" /> when the token is cancelled first.
    /// </summary>
    /// <param name="delay">The delay to wait. A non-positive delay completes immediately.</param>
    /// <param name="cancellationToken">A token observed while waiting.</param>
    /// <returns>A task that completes once the delay has elapsed.</returns>
    Task DelayAsync(TimeSpan delay, CancellationToken cancellationToken = default);
}
