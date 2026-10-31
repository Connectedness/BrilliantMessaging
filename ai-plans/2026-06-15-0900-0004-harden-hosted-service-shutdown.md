## Rationale

`TopologyRuntimeHostedService.StopAsync` currently stops topology runtimes sequentially in reverse registration order, but the first exception aborts the loop. A failure while stopping one runtime can therefore prevent earlier runtimes from draining and releasing their resources. Shutdown should remain deterministic and sequential, while making a best-effort attempt to stop every runtime and preserving all failures for the host to report.

The exception behavior should match the .NET Generic Host: rethrow a single failure unchanged and aggregate only when multiple runtimes fail. The hosted service should not log exceptions that it propagates because the host already reports shutdown failures; logging remains the responsibility of a runtime only for recoverable failures that it intentionally handles and suppresses. Hardening cleanup within individual runtime implementations, including `RabbitMqTopologyRuntime`, is out of scope for this slice.

## Acceptance Criteria

- [x] `TopologyRuntimeHostedService.StopAsync` attempts to stop every registered runtime sequentially in reverse registration order, even when one or more runtimes throw.
- [x] When exactly one runtime fails to stop, `StopAsync` rethrows the exact original exception instance without wrapping it and preserves its stack trace.
- [x] When multiple runtimes fail to stop, `StopAsync` throws an `AggregateException` containing the exact original exception instances in reverse shutdown order.
- [x] Cancellation exceptions from individual runtimes are collected like other shutdown failures and do not prevent the remaining runtimes from receiving `StopAsync`.
- [x] `TopologyRuntimeHostedService` does not log propagated shutdown failures or add an `ILogger` dependency.
- [x] Automated tests cover successful reverse-order shutdown, continued shutdown after a failure, single-failure propagation, multiple-failure aggregation, and cancellation during shutdown.

## Technical Details

Update `TopologyRuntimeHostedService.StopAsync` to retain its existing reverse indexed loop and sequential awaits. Wrap each runtime stop call in a `try`/`catch`, lazily collect caught exceptions, and continue to the next runtime regardless of the failure type. Do not introduce concurrent shutdown because runtimes may have ordering dependencies and the service explicitly promises reverse start order.

After every runtime has been attempted, return normally when no failures were collected. When there is one failure, use `ExceptionDispatchInfo.Capture(exception).Throw()` so callers observe the original exception type and stack rather than an unnecessary `AggregateException` or a reset stack. When there are multiple failures, throw an `AggregateException` with a message identifying topology runtime shutdown and with inner exceptions ordered by the order in which the runtimes were stopped.

Extend `TopologyRuntimeHostedServiceTests` with hand-crafted `ITopologyRuntime` test doubles that can record lifecycle calls and throw configured exceptions. Assert both the attempted stop order and the exact propagated exception shape. The single-failure test should assert reference identity with the configured exception instance. The multiple-failure test should assert that `AggregateException.InnerExceptions` contains the configured exception instances by reference and in stop-attempt order, proving that failures are neither wrapped nor reordered. Include a runtime that throws `OperationCanceledException` to verify that cancellation does not short-circuit cleanup; the original cancellation exception should still be propagated when it is the only failure and included in the aggregate when other failures also occur.
