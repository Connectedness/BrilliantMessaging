# RabbitMQ Exchange Policies

## Rationale

Plan 0070 brought the RabbitMQ queue policy knobs and the `Delete` resource-lifecycle modes into the topology builder as Configuration-as-Code, but deliberately deferred the matching exchange feature set: the shared `RabbitMqDeclareMode.Delete` and `RabbitMqBindingMode.Delete` values read correctly for exchanges, yet exchange deletion and exchange-binding deletion are wired to **throw** (`ArgumentOutOfRangeException`) at provisioning time. This plan closes that gap so exchanges and exchange bindings are first-class, Configuration-as-Code-managed resources with the same introduce → swap → `Delete` evolution workflow queues already have. It also adds the one missing exchange-side typed API — headers-binding match arguments — which today are reachable only through the raw `WithArgument(...)` escape hatch.

Exchanges are stateless routers, so the design follows the queue model closely rather than inventing a parallel one: an exchange has almost no declaration-time knob surface (its only arguments, `alternate-exchange` and `x-delayed-type`, are already typed), and deletion is unconditional and cascades, just like queue deletion.

## Acceptance Criteria

- [x] `RabbitMqTopologyProvisioner.ProvisionExchangeAsync` gains a real `Delete` arm (unconditional `ExchangeDeleteAsync`, `404`/already-absent treated as success for idempotency); the throwing `ArgumentOutOfRangeException` arm is removed. Exchange `Delete` stays in the existing exchange phase, so provisioning order remains exchanges → queues → bindings.
- [x] `RabbitMqTopologyProvisioner.ProvisionBindingAsync` gains a real `Delete` arm for `RabbitMqExchangeBindingDefinition` (`ExchangeUnbindAsync` with the binding's recorded arguments; not-found treated as success); the throwing `ArgumentOutOfRangeException` arm is removed.
- [x] The binding loop skips any binding referencing a `Delete`-mode exchange, mirroring the existing `Delete`-queue skip; `ProvisionBindingsAsync` receives the exchange definitions needed to build that skip set, and the two-pass Active-before-Delete ordering is unchanged.
- [x] A new `RabbitMqHeaderMatch` enum (`All`, `Any`, `AllWithX`, `AnyWithX`) plus typed `WithHeaderMatch(...)` and `WithHeader(...)` methods on **both** `RabbitMqExchangeBindingBuilder` and `RabbitMqQueueBindingBuilder`; `WithHeader("x-match", ...)` is rejected because `x-match` is the match-mode control argument, and the raw `WithArgument(...)` escape hatch remains.
- [x] The compiler hard-errors when an outbound target publishes to a `Delete`-mode exchange, mirroring the existing consumer-on-`Delete`-queue guard.
- [x] The compiler hard-errors when a binding configures header-match arguments (`x-match` / `WithHeader`) but its source exchange is not type `headers`, mirroring the existing `ValidateQueueKnobs` queue-type-incompatible guard.
- [x] The README "Evolving topology resources" subsection is extended to cover exchange evolution, including the rolling-deploy rule that old publishers must be drained from the old exchange before it is flipped to `Delete`, and the headers-binding typed API (including the `x-`-prefixed-header gotcha).
- [x] All newly public types and members carry XML doc comments, and the existing `RabbitMqDeclareMode.Delete` / `RabbitMqBindingMode.Delete` docs are updated so they no longer say exchange deletion is out of scope (CS1591 is the Release gate; `TreatWarningsAsErrors` stays clean).
- [x] Automated tests need to be written, including integration tests that pin down the broker behavior the design relies on (source/destination exchange-binding cleanup on exchange delete, idempotent re-delete, and `x-`-prefixed headers matching with `AllWithX` / not matching with plain `All`).
- [x] Code coverage stays above 93%, gathered with Microsoft.Testing.Extensions.CodeCoverage.

## Technical Details

### Exchange `Delete` provisioning (`RabbitMqTopologyProvisioner`)

Add a `DeleteExchangeAsync(IChannel channel, string exchangeName, CancellationToken)` helper shaped like the existing `DeleteQueueAsync` (`:322`): a `try` around `ExchangeDeleteAsync(exchangeName, ifUnused: false, cancellationToken: cancellationToken)` with a `catch (OperationInterruptedException ex) when (IsBrokerNotFound(ex))` no-op (reusing the existing `IsBrokerNotFound` predicate). `ProvisionExchangeAsync`'s `Delete` arm calls it instead of throwing.

The delete is **unconditional** (`ifUnused: false`), mirroring queue delete: unlike queues there is no pre-delete passive declare because exchanges hold no messages, so there is nothing to drain. The broker cascade-removes bindings owned by the deleted exchange, and the implementation treats any binding that names the deleted exchange as either source or destination as broker-cleaned-up; integration tests pin both source and destination exchange-to-exchange cases so the skip-set behavior is tied to observed RabbitMQ behavior. A `404 NOT_FOUND` (or an OK for an already-absent exchange — RabbitMQ's behavior here is pinned by integration test) is treated as success, so `Delete` is idempotent across restarts.

`ifUnused: true` is deliberately **not** used. The broker honors it (it refuses while the exchange has any source binding), but it would deadlock against the binding skip-set below: the skip-set leaves a `Delete` exchange's outgoing bindings in place on the assumption the delete cascades them, which an `ifUnused: true` delete would refuse to do. Routing continuity across a swap is instead preserved by the binding two-pass (create-before-destroy) and the multi-deploy introduce → swap → delete workflow.

No provisioning-order change is needed: exchange `Delete` runs in the existing first exchange loop (before queues and bindings), exactly as queue `Delete` runs in the queue phase. That loop already acquires a lease per step from the single-channel `DefaultRabbitMqChannelPool`, so a swallowed `404` that closes the channel self-heals.

### Exchange-binding `Delete` provisioning (`RabbitMqTopologyProvisioner`)

Add an `UnbindExchangeBindingAsync` helper mirroring `UnbindQueueBindingAsync` (`:230`), wrapping `ExchangeUnbindAsync(dest, source, routingKey, arguments, ct)` with the same not-found-as-success catch, using the binding's recorded `Arguments` so a headers exchange→exchange binding is matched. Replace the throwing `RabbitMqExchangeBindingDefinition { BindingMode: Delete }` arm in `ProvisionBindingAsync` with a call to it.

### Delete-exchange skip set (`ProvisionBindingsAsync` / `ProvisionBindingAsync`)

Change the `ProvisionAsync` call site and `ProvisionBindingsAsync` signature so the binding phase receives `_topology.Exchanges` as well as `_topology.Queues`. Build `HashSet<string> deleteExchangeNames` once from those exchanges next to the existing `deleteQueueNames` set, and pass it through to `ProvisionBindingAsync`. Extend the early-return guard: skip when a `RabbitMqQueueBindingDefinition.SourceExchangeName` is in the set, or a `RabbitMqExchangeBindingDefinition`'s source or destination is in the set. By the time the binding loop runs, the exchange phase has already deleted these exchanges and the broker has removed the relevant bindings, so this must be checked in both passes: an Active re-bind would target an absent exchange, and a Delete unbind would be redundant.

### Typed headers-binding API

New `RabbitMqHeaderMatch` enum (`All = 0`, `Any`, `AllWithX`, `AnyWithX`), XML-documented. The docs must note that `All`/`Any` exclude `x-`-prefixed headers from matching while `AllWithX`/`AnyWithX` include them — relevant because the framework's own header conventions use `x-` names (e.g. `x-tenant`), so the plain modes would silently match nothing on those headers. The string mapping (`all`/`any`/`all-with-x`/`any-with-x`) lives in the builder methods, mirroring how `RabbitMqQueueType` maps in the queue builder. Add `WithHeaderMatch(RabbitMqHeaderMatch)` and `WithHeader(string, object?)` to `RabbitMqExchangeBindingBuilder` and `RabbitMqQueueBindingBuilder`; both write into the existing `_arguments` dictionary and return `this`, with `WithHeader` reusing each builder's existing `RequireText` name validation. `WithHeader` rejects the exact name `x-match` with an `ArgumentException` because it is the match-mode control argument rather than a normal header predicate; users who need to bypass that typed-API guard can still call `WithArgument("x-match", ...)`. No definition-record or provisioner change is needed — the arguments already flow through `Build()` and the bind calls.

The typed API must not leave `x-match` to broker-default behavior, which is version-dependent and surprising. `WithHeader` writes a default `x-match` of `all` when none has been set yet, and `WithHeaderMatch` always overrides it; the XML docs state this so a single `WithHeader` call is unambiguous.

### Compiler guards (`RabbitMqTopologyCompiler`)

**Target → Delete exchange.** In `ValidateTarget` (`:1039`), after the existing unknown-exchange / type-mismatch checks, add: if the resolved exchange's `DeclareMode == RabbitMqDeclareMode.Delete`, append an error naming the target and the exchange ("… references exchange '…' declared with Delete mode; remove the target or change the exchange's declare mode"). This mirrors the consumer-on-`Delete`-queue guard (`:1309`). The existing `Enum.IsDefined` checks in `ValidateExchangeDefinitions` already accept the now-meaningful `Delete` value unchanged.

**Header-match args on a non-headers exchange.** In `ValidateBindings` (which already resolves each binding's source exchange via `exchangesByName`), add a guard mirroring `ValidateQueueKnobs`: if a binding's `Arguments` contain `x-match` (the marker the typed methods write) and the source exchange's `Type` is not `headers`, append a hard error naming the binding, the source exchange, and its actual type, with the remediation to remove the header-match configuration or use a headers exchange. This catches the silent footgun where `WithHeaderMatch`/`WithHeader` is set on a binding the broker would never match on. The guard keys on the `x-match` argument so it covers both the typed methods and a raw `WithArgument("x-match", …)`. It applies to both queue and exchange bindings.

### Evolving exchanges (workflow + README)

An exchange whose type or arguments must change is evolved with a new name, exactly as a queue is, because RabbitMQ refuses an in-place redeclare with different settings: a different `type` raises `530 NOT_ALLOWED`, while a different `durable`/`auto-delete`/arguments raises `406 PRECONDITION_FAILED` (both close the channel). The README "Evolving topology resources" subsection extends the existing introduce → swap → delete narrative to exchanges (delete is unconditional and the broker removes the bindings; `Delete` is idempotent across restarts; the management UI remains a valid alternative) and documents the headers-binding typed API with a short example.

The exchange workflow must call out the rolling-deploy hazard explicitly: deleting an exchange breaks any older application instance that is still publishing to it (`basic.publish` to a missing exchange is a `404 NOT_FOUND` channel error). The safe workflow is introduce the replacement exchange and bindings, deploy publishers so all live instances use the replacement, wait until old publishers are gone, then flip the old exchange to `Delete`.

### Out of scope

Automatic physical-name migration / broker-side manifest / orphan auto-cleanup, `rabbitmqctl` / management HTTP API integration, reading existing broker state to detect drift, and any new exchange *declaration* knobs beyond the already-typed `alternate-exchange` / `x-delayed-type`.

### References

Broker-behavior claims were verified against the official RabbitMQ / AMQP 0-9-1 documentation:

- [RabbitMQ — Channels](https://www.rabbitmq.com/docs/channels) — soft protocol errors (`404`/`406`/`530`) close the channel; recover by opening a new one. Validates carrying over the per-step channel-pool renewal from plan 0070.
- [RabbitMQ — Exchange-to-Exchange Bindings](https://www.rabbitmq.com/docs/e2e) — exchange-to-exchange bindings are directional source → destination bindings, and source bindings include both queue and exchange destinations. This is why the binding skip-set must consider both ends of an exchange binding when an exchange is deleted.
- [AMQP 0-9-1 Complete Reference](https://github.com/rabbitmq/amqp-0.9.1-spec/blob/main/docs/amqp-0-9-1-reference.md) — `exchange.delete` `if-unused` and `404`-on-absent semantics (used only to justify rejecting `if-unused: true`); `exchange.declare` raises `530 NOT_ALLOWED` on a different type and `406 PRECONDITION_FAILED` on different durable/arguments.
- [RabbitMQ — AMQP 0-9-1 Model, Headers Exchange](https://www.rabbitmq.com/tutorials/amqp-concepts#exchange-headers) — documents `x-match` values (`any`, `all`, `any-with-x`, `all-with-x`) and the rule that plain `any` / `all` exclude `x-`-prefixed headers.
