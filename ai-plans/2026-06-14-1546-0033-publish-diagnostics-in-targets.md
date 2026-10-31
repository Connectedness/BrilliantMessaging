# Publish Diagnostics in Targets

## Rationale

Direct `OutboundTarget<T>` publishing is an explicitly supported path, but diagnostics currently live in `MessagePublisher`. Move publish instrumentation into the target layer so publisher-mediated publishes and direct target publishes emit the same activities and metrics without double-counting.

## Acceptance Criteria

- [x] Publish diagnostics move from `MessagePublisher` into `OutboundTarget`, covering typed publishes, routable (routing-key) publishes, and raw serialized publishes.
- [x] `PublishSerializedAsync` becomes a public non-virtual template method over an abstract core method so raw publishes are always instrumented by the base target layer.
- [x] Typed `OutboundTarget<T>.PublishAsync` paths are instrumented by the target layer across the full publish (serialization through transport dispatch), so serialization failures and cancellations are captured exactly as today.
- [x] `MessagePublisher` keeps target resolution and explicit-target topology validation, then delegates without wrapping publish calls in diagnostics.
- [x] Publisher-mediated publishes and direct target publishes produce one activity, one attempt count, and one duration measurement per publish attempt, plus exactly one failure count per non-cancellation failed publish attempt.
- [x] Existing success, cancellation, serialization failure, and delivery failure diagnostic tags remain consistent with current publisher-owned diagnostics.
- [x] Automated tests need to be written.

## Technical Details

Move the diagnostics template from `MessagePublisher.PublishWithDiagnosticsAsync` to the non-generic `OutboundTarget` base. The base target should own activity creation, common tags, `PublishAttempts`, `PublishFailures`, `PublishDuration`, cancellation outcome handling, and delivery failure reason tagging. Keep the observable tag names and metric dimensions compatible with the current `OutboundDiagnostics` behavior.

Change raw publish dispatch so `OutboundTarget.PublishSerializedAsync` is a public non-virtual template method and invokes diagnostics around an abstract `PublishSerializedCoreAsync`. Update all concrete targets and test doubles that currently override `PublishSerializedAsync`, including RabbitMQ targets, benchmark targets, `RecordingTarget`, `NonRoutableRecordingTarget`, and `ThrowingTarget`, to override the core method instead. The base's raw path derives the `usf.outbound.message.type` tag from the target (reproducing the current `GetMessageTypeName(target)`), distinct from the typed path's discriminator.

For typed publishing, keep validation and serialization behavior in `OutboundTarget<T>.PublishAsync` / `PublishCoreAsync`, and instrument the `PublishCoreAsync` path with the target-owned diagnostics. The instrumented region begins at serialization and runs through the transport-specific `PublishTypedCloudEventAsync` dispatch; discriminator and data-schema resolution happen *before* the instrumented region starts, so the direct-publish path (where `type` is resolved inside `PublishCoreAsync`) and the publisher-mediated path (where the discriminator is resolved upstream and passed in) behave identically and metadata-resolution failures are not counted as publish failures on either path. Serialization failures (`outcome="failure"`, no delivery reason) and cancellations are recorded exactly as the current publisher-owned diagnostics do. Use the target's discriminator lookup for typed message type names and preserve existing serialization exception behavior.

Simplify `MessagePublisher` after target-owned diagnostics are in place. `PublishRawAsync` should validate the explicit target and serialized message shape, run `ValidateExplicitTargetTopology`, and call `target.PublishSerializedAsync`. Typed publishing should resolve the target, validate explicit topology, verify target type compatibility, gather discriminator/data schema as needed, and dispatch without starting activities or recording metrics. The current routing branch is preserved: when no routing key is supplied it calls `typedTarget.PublishAsync`, and when a routing key is supplied it calls `IOutboundRoutableTarget<T>.PublishAsync` with the routing key. Both overloads funnel through `PublishCoreAsync`, which is the single instrumented funnel, so neither branch should be wrapped in publisher-side diagnostics.

Update diagnostics tests to cover direct typed target publishing, direct raw target publishing, publisher-mediated typed publishing, publisher-mediated raw publishing, cancellation, serialization failures, delivery failures, and absence of nested activities or doubled metrics when publishing through `IMessagePublisher`.

Scope: this plan overlaps #30 / `0030-capability-typed-routing-keys` on `OutboundTarget<T>.PublishCoreAsync`, `MessagePublisher`, `RecordingTarget`, and the RabbitMQ targets; implement it after #30 so `PublishCoreAsync` is the shared `protected` funnel that this plan instruments — the two compose cleanly once sequenced.
