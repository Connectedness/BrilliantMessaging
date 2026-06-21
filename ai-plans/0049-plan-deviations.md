# 0049 — Plan Deviations

This file records the changes made **after** the initial implementation of
[`0049-inbound-diagnostics.md`](./0049-inbound-diagnostics.md) (commit `96d9a08`). They came out of an
architectural review of that commit and are documented here because they refine — and in two places correct — the
behaviour the plan described. This is not a plan; it is a change log for the four follow-up commits.

The acceptance criteria in the plan remain satisfied; these deviations adjust *how* a few of them are met.

---

## 1. `InboundDiagnosticsMiddleware` — single point of truth for the base tags (`bd39088`)

**What the initial implementation did.** The middleware built the base `TagList` for the metrics and then set the
same four tags on the consumer activity through four separate, literal `SetTag(...)` calls. The tag list therefore
existed twice in the method.

**Change.** The activity tags are now applied by iterating the existing `baseTags`, so the tag set is declared once
and reused for both metrics and the activity:

```csharp
foreach (var tag in baseTags)
{
    activity.SetTag(tag.Key, tag.Value);
}
```

The `if (activity is not null)` guard is retained, so nothing extra runs on the no-listener path, and iterating a
`TagList` uses its struct enumerator (no allocation).

**Files.** `src/Bmf.Core/Messaging/Inbound/InboundDiagnosticsMiddleware.cs`.

---

## 2. `TraceContextHeadersExtractResult.Baggage` is now `IReadOnlyDictionary<string, string?>` with value-object equality (`a06c0be`)

**What the initial implementation did.** `Baggage` was typed `IEnumerable<KeyValuePair<string, string?>>`, and the
record relied on the compiler-synthesized equality — which compares an `IEnumerable` member **by reference**. Two
results carrying the same baggage but in different sequence instances compared unequal, which is misleading for a
type that advertises value semantics.

**Change.** Two related refinements:

- **Value-object equality.** `Equals`/`GetHashCode` are now hand-written so two results are equal when their
  `TraceParent` and `TraceState` match ordinally and their baggage describes the same key/value pairs, with an
  order-independent (XOR) baggage hash — mirroring the existing `CloudEventEnvelope` precedent.
- **`IReadOnlyDictionary` baggage.** `Baggage` was changed from `IEnumerable<KeyValuePair<string, string?>>` to
  `IReadOnlyDictionary<string, string?>`. This models baggage as what it is (a unique-keyed map), removes the
  duplicate-key normalization that an enumerable would have required, and lets the equality/hash methods enumerate
  the concrete backing `Dictionary` through its **struct enumerator** so they allocate nothing. Iterating through
  the `IReadOnlyDictionary` interface would box the enumerator; a foreign read-only dictionary (a caller's
  deliberate downcast/choice) takes an allocating interface fallback.
- **Boundary materialization.** `TraceContextHeaders.Extract` now materializes an owned
  `Dictionary<string, string?>(StringComparer.Ordinal)` snapshot once (helper `ToBaggageMap`), collapsing any
  repeated wire key (last wins). The previous empty-case `Array.Empty<...>()` fallback is gone; an empty result is
  simply an empty map.

**Public breaking change.** Changing the `Baggage` property type is a breaking change. It is taken now under the
pre-1.0 allowance (`AGENTS.md`) and should be called out in the 0.1.0 release notes — alongside the
`TraceContextHeaders` namespace move that the plan already made.

**Files.** `src/Bmf.Core/Messaging/TraceContextHeadersExtractResult.cs`,
`src/Bmf.Core/Messaging/TraceContextHeaders.cs`, and
`tests/Bmf.Core.Tests/Messaging/TraceContextHeadersExtractResultTests.cs` (new).

---

## 3. Inbound `process.attempts` is recorded up front, without the outcome tag (`5e83d32`)

**What the initial implementation did.** The middleware recorded `bmf.inbound.process.attempts` in the `finally`
block, tagged with the `outcome`. That diverged from the outbound path, where `OutboundTarget` records
`bmf.outbound.publish.attempts` **before** the operation and **without** an outcome tag. The two counters
therefore had different shapes (the inbound one carried an extra `outcome` dimension and counted completions
rather than starts), undercutting the plan's goal of symmetric, joinable inbound/outbound telemetry.

**Change.** The middleware now records the attempt **before** invoking `next`, tagged with the base tags only
(no `outcome`):

```csharp
InboundDiagnostics.ProcessAttempts.Add(1, baseTags);
try { ... }
```

The `finally` block now records only `process.duration` (still tagged with the outcome). The pre-pipeline failure
path in `RabbitMqTopologyRuntime.ProcessDeliveryAsync` was adjusted to match: it adds `process.attempts` with the
bare tags, then appends `outcome = "failure"` for the `process.failures` measurement only. As a result,
`bmf.inbound.process.attempts` now has the same cardinality as `bmf.outbound.publish.attempts`, the attempt is a
true "started" count, and the `attempts ≥ failures` invariant still holds.

**Files.** `src/Bmf.Core/Messaging/Inbound/InboundDiagnosticsMiddleware.cs`,
`src/Transports/Bmf.Transport.RabbitMq/Inbound/RabbitMqTopologyRuntime.cs`, and the corresponding test updates
(`InboundDiagnosticsMiddlewareTests`, `RabbitMqTopologyRuntimeTests`), including a new test that asserts the
attempt is recorded before `next` runs and without an outcome tag.

---

## 4. `RabbitMqTopologyRuntime` treats delivery-token cancellation as `cancelled`, consistently with the middleware (`d92413d`)

**What the initial implementation did.** The runtime's requeue path was guarded by
`catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)`, i.e. it only treated *graceful
shutdown* as a cancellation. The middleware, however, classifies cancellation on the **linked** token
(`context.CancellationToken`, which combines the delivery's `eventArgs.CancellationToken` and the stopping token).
So a cancellation originating from the delivery token alone (e.g. consumer/channel teardown, not shutdown) was
reported by the middleware as `cancelled`, but the runtime fell through to its general failure catch — logging an
error and NACK-ing with `requeue: false`. Telemetry and operational handling disagreed.

**Change.** The requeue guard now keys on the same linked token the middleware uses:

```csharp
catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
```

A delivery-token cancellation is now requeued (`requeue: true`) and counts no failure metric, matching the
middleware's `cancelled` outcome. The early pre-scope shutdown check still uses `stoppingToken` as before.

**Files.** `src/Transports/Bmf.Transport.RabbitMq/Inbound/RabbitMqTopologyRuntime.cs` and a new
`RabbitMqTopologyRuntimeTests` case covering delivery-token cancellation (requeue, no failure metrics).

---

## Verification

After all four commits: the solution builds warning-clean in Release (`TreatWarningsAsErrors`), and the unit suites
pass — `Bmf.Core.Tests` (129) and `Bmf.Transport.RabbitMq.Tests` (105).
