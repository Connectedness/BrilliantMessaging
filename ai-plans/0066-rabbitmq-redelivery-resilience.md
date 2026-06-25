# RabbitMQ Inbound Redelivery Resilience

## Rationale

BrilliantMessaging 0.1.0 has no inbound redelivery resilience: `FrameworkMessageAcknowledgementMiddleware` nacks `requeue: false` on any handler failure (one-and-done) and `requeue: true` only on cancellation. There is no notion of "this failure is transient, redeliver it" versus "this is poison, drop it" — the one decision the broker cannot make for us.

This plan adds that decision, makes it the default rather than opt-in, and lets the broker own everything else (counting, back-off, dead-lettering):

- **Quorum is the default queue type** (RabbitMQ's recommendation since 4.0). Its built-in delivery limit (default 20, which this plan leaves at the broker default and does not set) bounds any redelivery loop — both the broker's own no-ack loop and a client-requested `requeue: true`. That backstop is what makes retry-by-default safe without a client-side counter. Classic stays available via `AsClassicQueue()`.
- **There is always an effective classifier; on quorum it retries by default.** A handler failure on a quorum queue nacks `requeue: true` unless the exception is poison (a `MessageDeserializationException` from the resolved endpoint's deserialization step), so transient failures get up to the broker's ~20 immediate redeliveries before being dead-lettered/dropped. Users tune *which* exceptions are poison via `ShouldRetry` and the marker exceptions. (A genuinely *unrecognized* message resolves no endpoint and never reaches a classifier — it is rejected upstream; see below.)
- **The client only classifies — it never counts, caps, or delays.** No `MaxAttempts`, no `Task.Delay` (which at the default `PrefetchCount` of 1 would stall the consumer). The retry count is the quorum delivery limit; back-off, where wanted, is broker-side (quorum delayed retry 4.3+, or a classic DLX + TTL wait queue). These tunable broker knobs are RabbitMQ policies (management CLI today, management HTTP API / Configuration-as-Code in a future plan), not queue arguments and not client code. This plan writes only `x-queue-type`.

Retries are immediate (no back-off), so the default suits transient/contended failures (deadlocks, races, brief unavailability) that recover on a quick retry; tuning the count and adding back-off is deferred to broker-policy support. `DeliveryAttempt` is not read by the runtime decision (there is no cap); it stays populated for observability via the existing `GetDeliveryAttempt`, unchanged.

### Two deliberate pre-1.0 breaking changes

**Quorum default.** A quorum queue must be durable and cannot be `exclusive` or `auto-delete`. Combining a classic-only flag (`DurableQueue(false)`, `ExclusiveQueue()`, `AutoDeleteQueue()`) with quorum is a hard compile error directing the user to `AsClassicQueue()`. The framework declares no internal exclusive/auto-delete queues, so its own paths are unaffected. The operational break: an existing classic queue cannot be redeclared as quorum — provisioning fails with `406 PRECONDITION_FAILED`. Upgrading users opt those queues into classic or migrate (drain, delete, redeclare).

**Retry-by-default on quorum.** Handler failures on a quorum queue now redeliver (up to the broker limit) instead of being dropped one-and-done. This assumes handlers are idempotent; non-idempotent handlers should narrow `ShouldRetry` or throw `RejectMessageException`. The README and release notes call out both changes.

### Default behaviour is resolved by queue type

Retrying with `requeue: true` is only safe where the broker bounds the loop — the quorum delivery limit. A plain classic queue has no backstop the compiler can see, so the effective default differs by queue type, resolved at compile time:

- **Quorum** → default classifier **retry-unless-poison**.
- **Classic / Unknown** → default classifier **reject-all** (`requeue: false`, exactly 0.1.0 behaviour).

Because retries are unavailable on classic/unknown, *explicitly* configuring a classifier there (`WithRedelivery(...)`) is a hard compile error, pointing the user at the quorum default or at asserting the real type with `QueueType(Quorum)`. The compiler already matches each consumer to its queue (`ValidateConsumers` against `queuesByName`), so it reads `x-queue-type` with no user input; the only blind spot is a passively/externally declared queue, covered by `QueueType(...)`.

## Acceptance Criteria

- [ ] `RabbitMqQueueBuilder` declares queues as quorum by default (`x-queue-type = quorum`); `AsClassicQueue()` opts out and `AsQuorumQueue()` stays an idempotent setter.
- [ ] Combining a classic-only flag (`DurableQueue(false)`, `ExclusiveQueue()`, `AutoDeleteQueue()`) with a quorum queue is a hard topology-compilation error naming the queue and pointing at `AsClassicQueue()`.
- [ ] This plan sets no `x-delivery-limit` / `x-delayed-retry-*` arguments and adds no client-side attempt cap; the only resilience-relevant declaration argument is `x-queue-type`. The retry count is the quorum broker default (~20); delivery-limit / delayed-retry / dead-letter tuning is out of scope (operator policies).
- [ ] A transport-neutral `RedeliveryClassifier` (record) in `BrilliantMessaging.Core.Messaging.Inbound` captures a retryable-vs-poison classification of a handler exception — no cap, no delay, no exhaustion action — exposing `bool ShouldRetry(Exception failure)`. Core provides two well-known instances: a **retry-unless-poison** default and a **reject-all**. A `RedeliveryClassifierBuilder` (`IBuildable<RedeliveryClassifier>`, like the other builders) configures a custom one.
- [ ] There is always an effective classifier — no opt-in / legacy branch. The compiler resolves the default by queue type: **quorum → retry-unless-poison**, **classic/unknown → reject-all** (`requeue: false`, exactly 0.1.0 behaviour). Cancellation still nacks `requeue: true` regardless.
- [ ] On quorum the classifier is configurable consumer-wide via `RabbitMqInboundConsumerBuilder.WithRedelivery(Action<RedeliveryClassifierBuilder>)` and overridden per endpoint via `RabbitMqInboundHandlerBuilder.WithRedelivery(...)`; the effective classifier is carried on the resolved `InboundEndpoint` (handler wins, else consumer, else the queue-type default).
- [ ] The default retry-unless-poison classifier treats `MessageDeserializationException` as poison and other handler exceptions as retryable. (Unrecognized messages never reach the classifier — they resolve no endpoint and are rejected by the pre-pipeline path; the classifier's poison set is therefore only the reachable deserialization failure, not "unrecognized/contract-mismatch".) A `ShouldRetry(Func<Exception,bool>)` predicate and marker exceptions (`RejectMessageException`, `RetryMessageException`) override the classification. On a reject-all (classic/unknown) endpoint every handler failure nacks `requeue: false` and the retry marker has no effect.
- [ ] The decision lives in the inbound acknowledgement middleware: a non-cancellation failure nacks `requeue: ShouldRetry(ex)`; cancellation still nacks `requeue: true`; pre-pipeline failures with no resolved endpoint stay immediate poison rejects. The decision reads no delivery count and awaits no delay.
- [ ] The compiler auto-detects each consumer's queue type from the in-topology `x-queue-type` (quorum by default); explicitly configuring a classifier (`WithRedelivery(...)`) on a `Classic` or `Unknown` queue is a hard compile error (no broker backstop for `requeue: true`), lifted by `QueueType(Quorum)`.
- [ ] `RabbitMqInboundConsumerBuilder.QueueType(RabbitMqQueueType)` lets users assert the type for passively/externally declared queues the compiler cannot inspect; an explicit type that contradicts the actively-declared `x-queue-type` is a hard error.
- [ ] `DeliveryAttempt` stays populated for observability only; `GetDeliveryAttempt` (`x-delivery-count` → `x-death` → `redelivered`) is unchanged. No header switch and no off-by-one verification are in scope.
- [ ] The README gains a "Resilience and redelivery" section: the `RedeliveryClassifier` API, the quorum default (incl. the `406` migration warning) and the retry-by-default change, auto-detection, the immediate-redelivery-up-to-the-broker-limit behaviour and its no-back-off caveat, the classic reject-all default and the guard (incl. the note that a retry marker is a no-op on classic and that `QueueType(Quorum)` on an actually-classic queue re-enables an unbounded loop), and a pointer to tuning broker knobs as RabbitMQ policies out-of-band.
- [ ] All newly public types and members carry XML doc comments (CS1591 is the Release gate; `TreatWarningsAsErrors` stays clean).
- [ ] Tests: unit tests for the classification decision and marker/predicate overrides, the queue-type-resolved default (quorum retries, classic rejects), the explicit-classifier-on-classic/unknown guard, the consumer-vs-handler reconciliation, and queue-type auto-detection; an integration test on a real quorum queue — bound to a dead-letter exchange so the terminal outcome is a deterministic positive assertion rather than a timing-sensitive absence check — asserting a retryable failure is requeued and, once the broker's delivery limit is hit, lands in the dead-letter queue, while a poison failure is rejected on first delivery (the fixture already runs RabbitMQ 4.3.1 — see `RabbitMqFixture` / `DockerImages`).

## Technical Details

### Core (`BrilliantMessaging.Core.Messaging.Inbound`)

`RedeliveryClassifier` (immutable record) exposes `bool ShouldRetry(Exception failure)` and handles the markers internally: `RejectMessageException` → `false`, `RetryMessageException` → `true`, otherwise the configured predicate. Core provides two well-known instances — the **retry-unless-poison** default (predicate: poison for `MessageDeserializationException`, retryable otherwise) and **reject-all** (`ShouldRetry` always `false`, markers included). `RedeliveryClassifierBuilder` (`IBuildable<RedeliveryClassifier>`) builds a custom predicate-based one.

`InboundEndpoint` gains a `RedeliveryClassifier` property defaulting to reject-all (so endpoints that never set it keep 0.1.0 behaviour). `FrameworkMessageAcknowledgementMiddleware` becomes classification-aware: on a non-cancellation exception it nacks `requeue: endpoint.Classifier.ShouldRetry(ex)`; cancellation still always `requeue: true`. Core reads only the exception + classifier — no count, no delay.

### RabbitMQ (`BrilliantMessaging.Transport.RabbitMq`)

Add a `RabbitMqQueueType` enum (`Classic`, `Quorum`, `Unknown`). The compiler resolves each consumer's queue type — explicit `QueueType(...)`, else the declared `x-queue-type`, else `Unknown` — and uses it to (a) pick the effective default classifier (quorum → retry-unless-poison, classic/unknown → reject-all) and (b) drive the explicit-configuration guard. Nothing count-related is baked into `RabbitMqInboundConsumer`.

`RabbitMqQueueBuilder` defaults to quorum in its constructor (alongside `Durable = true`); add `AsClassicQueue()`, keep `AsQuorumQueue()` idempotent. Add `WithRedelivery(...)` / `QueueType(...)` to `RabbitMqInboundConsumerBuilder` and `WithRedelivery(...)` to `RabbitMqInboundHandlerBuilder`, threading the classifier through the consumer/handler definitions; the compiler reconciles handler-over-consumer-over-queue-type-default and bakes the result onto the endpoint. Existing tests asserting classic defaults or combining `DurableQueue(false).ExclusiveQueue().AutoDeleteQueue()` (`RabbitMqDeclarationBuilderTests`, `AddRabbitMqPublishTopologyTests`, `AddRabbitMqConsumeTopologyTests`) update to call `AsClassicQueue()` and expect the quorum default.

### Validation

- `ValidateQueueDefinitions`: hard error when a quorum queue also sets a classic-only flag, naming the queue and pointing at `AsClassicQueue()`.
- `ValidateConsumers`: hard error when an explicit `QueueType(...)` contradicts the declared `x-queue-type`.
- `ValidateConsumers`: hard error when a classifier is explicitly configured (`WithRedelivery(...)`, consumer-wide or per-handler) on a `Classic` or `Unknown` queue, lifted by `QueueType(Quorum)`.

### Out of scope

Delayed retry applies automatically to `basic.nack` over AMQP 0.9.1, so no client API change is needed for back-off (`RabbitMQ.Client` 7.2.1 exposes only `BasicNackAsync` / `BasicRejectAsync`). Modeling/applying RabbitMQ policies (delivery limit, delayed retry, dead-lettering, TTL, length limits) is operator/Configuration-as-Code work for a future plan, not part of a consumer's topology definition; the exhaustion action (drop vs dead-letter) follows from whether the queue routes to a DLX, an operator concern. If sub-second classic smoothing or tunable immediate-retry counts are ever wanted, they return as explicit, documented opt-ins — not defaults.
