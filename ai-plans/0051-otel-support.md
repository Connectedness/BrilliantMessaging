## Rationale

BMF already instruments both directions through the BCL-native `ActivitySource`/`Meter` primitives that OpenTelemetry consumes directly — outbound today, inbound once `ai-plans/0049-inbound-diagnostics.md` lands. But the spans and metrics are annotated with BMF-private `bmf.outbound.*`/`bmf.inbound.*` attributes. To generic tooling (Jaeger, Tempo, Grafana, Datadog, Azure Monitor) those are opaque strings: nothing classifies the span as "a publish to a RabbitMQ exchange," so the built-in messaging dashboards, service-map producer→broker→consumer edges, and "latency by destination" views stay empty. Propagation (slice 0001-8) *links* the producer and consumer spans; semantic attributes *label* them — they are complementary halves, and we currently ship only the first.

This plan adopts the OpenTelemetry **`messaging.*` semantic conventions** as the attribute and metric vocabulary on every BMF messaging span and instrument, **replacing** the `bmf.*` scheme rather than dual-emitting it (we are pre-1.0; carrying a private vocabulary into a stable release only to break it later is the worse trade). It also ships a thin **`Bmf.OpenTelemetry`** integration project so users wire BMF's sources and meters into their `TracerProvider`/`MeterProvider` in one line. `Bmf.Core` stays free of any OpenTelemetry *package* dependency — only the standardized attribute *names* are adopted; the wiring lives in the optional integration package, exactly as `HttpClient`/ASP.NET Core keep instrumentation in the BCL and registration in `OpenTelemetry.Instrumentation.*`.

This builds on `0049-inbound-diagnostics.md` and converts both the existing outbound path and the new inbound path; **0049 should land first**.

## Acceptance Criteria

- [x] Every BMF messaging span and metric is annotated with OpenTelemetry `messaging.*` semantic-convention attributes; the `bmf.outbound.*` and `bmf.inbound.*` span tags and metric tag keys are **removed, not dual-emitted**.
- [x] The producer and consumer activities use `ActivityKind.Producer`/`ActivityKind.Consumer` and a low-cardinality `DisplayName` formed per the **pinned spec's** messaging span-name convention (destination + operation — illustratively `orders.created send` / `orders.created process`; confirm the exact token order and `messaging.operation.name` value against the pinned version, since both have moved across releases). The destination component is known at activity-start time, so the name is set when the activity starts.
- [x] Outbound spans/metrics carry `messaging.system`, `messaging.operation.type=send` (+ `messaging.operation.name`), `messaging.destination.name` (the exchange), `messaging.rabbitmq.destination.routing_key` (when a routing key is present), `messaging.message.id` (the CloudEvents `id`), and `messaging.message.body.size`. On failure `error.type` is set and the activity status is `Error` with the exception recorded.
- [x] Inbound spans/metrics carry the same attribute set with `messaging.operation.type=process`, sourcing `messaging.destination.name` from the consumed source and `messaging.message.id`/body size from the delivery.
- [x] Metric instruments follow the messaging conventions: `messaging.client.sent.messages` (outbound counter), `messaging.client.consumed.messages` (inbound counter), and `messaging.client.operation.duration` (histogram, unit **`s`**) replace `bmf.outbound.publish.{attempts,failures,duration}` and `bmf.inbound.process.{attempts,failures,duration}`. Failures are represented by the `error.type` dimension on the message counter rather than a separate failures instrument; the duration unit changes from milliseconds to **seconds** per the convention.
- [x] `error.type` draws from a **bounded vocabulary** so it is safe as a metric dimension: known delivery-failure reasons map to stable tokens (`nacked`/`returned`/`timeout`), and any other failure (arbitrary handler/processing exception) maps to a single catch-all token (`_OTHER`) — never the raw exception type name. The unbounded, specific exception type may appear only on the **span** (where cardinality is harmless), not on any metric dimension.
- [x] **Cancellation is not an error.** A graceful-shutdown cancellation (the outbound `cancelled` outcome, the inbound delivery-token cancellation, the pre-pipeline shutdown requeue) leaves `error.type` **absent** on both the span and the metric — so normal deploy/scale-down activity does not inflate the messaging error-rate panels. It is distinguished, if at all, only by span status, not by `error.type`.
- [x] The topology-provisioning instruments (`bmf.outbound.topology.provisioning.*`) are explicitly **out of scope** and keep their `bmf.*` names — provisioning is not a messaging operation and no `messaging.*` convention applies.
- [x] The `messaging.*` attribute-name constants are defined in `Bmf.Core` (no OpenTelemetry package dependency) and are `public` so transports reuse them. Transport-specific values (system, destination, routing key) are supplied through extension points on `OutboundTarget`/`TransportMessage`; `Bmf.Core` sets the transport-neutral attributes uniformly, and the transport contributes its specific ones.
- [x] A new `Bmf.OpenTelemetry` project provides `AddBmfInstrumentation` extension methods over `TracerProviderBuilder` and `MeterProviderBuilder` that register the `Bmf.Outbound`/`Bmf.Inbound` sources and meters. It references only `OpenTelemetry.Api`, reuses the source/meter-name constants from `Bmf.Core` rather than duplicating string literals, and ships with package metadata/README consistent with the other `src` projects.
- [x] `Bmf.Core` (and the RabbitMQ transport) take **no** OpenTelemetry package reference; instrumentation stays purely `System.Diagnostics`.
- [x] XML docs are updated: the diagnostics types document the emitted `messaging.*` attribute set and link the **pinned** OTel messaging spec version (see References); the `MessagingSemanticConventions` constants each cite the spec; the `Bmf.OpenTelemetry` extension methods explain what they register and that they are the supported one-line wiring.
- [x] Automated tests are written.

## Technical Details

**Attribute-name constants (`Bmf.Core`).** Add a public static `MessagingSemanticConventions` class holding the `messaging.*` and `error.type` key constants (the SDK has no need to be referenced for names; defining our own also avoids the churny experimental `OpenTelemetry.SemanticConventions` package). Pin the class — in its XML docs and a comment — to the specific semantic-conventions version the keys are taken from (see References), since the messaging group has renamed attributes across versions (`messaging.operation` → `messaging.operation.type`, destination renames); bumping the version is then a deliberate, reviewable edit. The existing `*TagName` constants on `OutboundDiagnostics`/`InboundDiagnostics` are removed and their usages repointed here. Keys used:

| Attribute | Outbound value | Inbound value |
|---|---|---|
| `messaging.system` | transport system (`rabbitmq`) | same |
| `messaging.operation.type` | `send` | `process` |
| `messaging.operation.name` | `send` | `process` |
| `messaging.destination.name` | exchange | consumed source (queue/exchange) |
| `messaging.rabbitmq.destination.routing_key` | routing key (when present) | delivery routing key |
| `messaging.message.id` | CloudEvents `id` | CloudEvents `id` / transport message id |
| `messaging.message.body.size` | `envelope.Data.Length` | delivery body length |
| `error.type` | bounded token on failure (`nacked`/`returned`/`timeout`/`_OTHER`); absent on success and cancellation | bounded token on failure (`_OTHER`, or a known reason); absent on success and cancellation |

`messaging.system` and the RabbitMQ-namespaced routing-key key are transport-specific; everything else is transport-neutral and set by Core. The inbound `messaging.destination.name` exchange-vs-queue choice follows the RabbitMQ semconv page (see References) rather than being guessed.

**Deliberately deferred attributes (not overlooked).** Three attributes from the broader messaging convention are intentionally out of scope here:
- `server.address` / `network.protocol.*` (broker connection facts) — they require host/port details the target and transport message do not cleanly expose today; a later enrichment slice can add them.
- `messaging.message.conversation_id` — there is no first-class source for it. BMF has only a transport-level correlation-id passthrough (`SerializedMessage.CorrelationId` on the raw path, `TransportMessage.CorrelationId` on receive); the typed publish path sets no correlation id and nothing propagates an inbound id to outbound messages. A first-class correlation/conversation-id feature is its **own plan**; `conversation_id` is wired here once that lands.

**Outbound conversion.** `OutboundTarget` keeps owning the activity/metrics funnel (`StartPublishDiagnostics`/`PublishDiagnostics`). Add transport-supplied extension points so the base can set the convention attributes: a `MessagingSystem` member (defaulting to `TransportName`, which is already `"rabbitmq"`) and a `DestinationName` member that `RabbitMqOutboundTarget<TMessage>` overrides to return its `_exchangeName`. The base sets `messaging.system`/`operation.*`/`destination.name`/`message.*` and the `DisplayName` at activity start; the routing key (resolved per publish inside the concrete target) and any other transport specifics are set on `Activity.Current` at dispatch — the same "current activity is the producer span at the binding" insight used for trace injection. `error.type` is set per the bounded-vocabulary rule below (replacing the `bmf.outbound.delivery.failure.reason`/`outcome` tags). Instruments on `OutboundDiagnostics` become `messaging.client.sent.messages` and `messaging.client.operation.duration` (seconds).

**Inbound conversion.** `InboundDiagnosticsMiddleware` (from 0049) keeps owning the consumer activity/metrics. Expose the transport semantics on `TransportMessage`: a `MessagingSystem` property (default `TransportName`) and reuse `Source` for `messaging.destination.name`; surface the routing key (RabbitMQ already has it on `RabbitMqTransportMessage`) either via an optional base property or a transport-set tag, consistent with the outbound split. The middleware sets the same attribute set with `operation.type=process`, the `DisplayName` from the source, and `error.type` per the bounded-vocabulary rule below. Instruments on `InboundDiagnostics` become `messaging.client.consumed.messages` and `messaging.client.operation.duration` (seconds). The pre-pipeline failure path in `RabbitMqTopologyRuntime.ProcessDeliveryAsync` (0049) increments `messaging.client.consumed.messages` with a bounded `error.type` instead of the dropped `process.failures` counter — and its graceful-shutdown requeue branch sets no `error.type` (it remains a non-error cancellation, exactly as 0049 already excludes it from the failures counter).

**Metric-shape decision (call out for review).** Adopting the messaging metric conventions means there is **no separate failures counter**: a failure is the same `sent`/`consumed` counter increment carrying an `error.type` dimension (failure rate = the `error.type`-present slice). This supersedes the `attempts`/`failures` pair — and the attempts ≥ failures invariant — that 0049 and the current outbound code establish. It is the coherent OTel end state (and what backends' messaging panels expect), but it is the one genuinely behavioral change here, so it is flagged rather than buried. The `Bmf.Outbound`/`Bmf.Inbound` `ActivitySource`/`Meter` *names* are unchanged — they identify the instrumentation scope, not semantic attributes, and remain the subscription handles.

**`error.type` discipline (the guardrails that make the shape safe).** Folding failures into a dimension on the headline counter is only safe if that dimension stays low-cardinality, so two rules are non-negotiable:
- *Bounded vocabulary.* `error.type` is drawn from a closed set: the known delivery-failure reasons map to stable tokens (`nacked`/`returned`/`timeout`, from `MessageDeliveryFailureReason`), and every other failure — any handler/processing exception — maps to the single catch-all `_OTHER`. The raw, unbounded exception type name is **never** used on a metric dimension; it may be recorded on the span only (e.g. via `Activity` status/exception event), where high cardinality is harmless. A central helper (alongside `MessagingSemanticConventions`) maps an outcome/exception to its bounded `error.type`, shared by the outbound and inbound paths so the vocabulary stays in one place.
- *Cancellation is not an error.* A graceful-shutdown cancellation sets **no** `error.type` on either the span or the counter; it counts as an ordinary `messaging.client.{sent,consumed}.messages` increment with the dimension absent (the same way a success does). This keeps deploy/scale-down churn out of the error-rate panels. The pre-existing `cancelled`-isn't-a-`failure` rule from 0049 carries straight over — only the representation changes (absent dimension instead of a skipped counter).

**`Bmf.OpenTelemetry` project (new, `src/Bmf.OpenTelemetry/`).** Targets `netstandard2.0` like the rest of `src`, references `Bmf.Core` and only `OpenTelemetry.Api` (which defines `TracerProviderBuilder.AddSource` and `MeterProviderBuilder.AddMeter`, so no SDK dependency is forced on consumers). Add the `OpenTelemetry.Api` version to `Directory.Packages.props`. Provide:

```csharp
public static TracerProviderBuilder AddBmfInstrumentation(this TracerProviderBuilder builder) =>
    builder.AddSource(OutboundDiagnostics.ActivitySourceName, InboundDiagnostics.ActivitySourceName);

public static MeterProviderBuilder AddBmfInstrumentation(this MeterProviderBuilder builder) =>
    builder.AddMeter(OutboundDiagnostics.MeterName, InboundDiagnostics.MeterName);
```

In the current code the `Meter` is constructed as `new(ActivitySourceName)`, so the meter name today equals the activity-source name and there is no `MeterName` constant. Introduce a `MeterName` constant on each diagnostics class (initial value unchanged, i.e. equal to `ActivitySourceName`), use it both to construct the `Meter` and in `AddMeter`, so the registration does not pass a constant literally named `ActivitySourceName` to a meter API and the two names can diverge later without touching callers. Even without this package a user can `AddSource("Bmf.Outbound")`/`AddMeter("Bmf.Outbound")` directly; the package is the discoverable, named convenience and the home for any future per-builder options. Add the project to the solution and CI.

**Testing notes.** Assert the emitted attribute set and instrument names/units via `ActivityListener`/`MeterListener` on `Bmf.Outbound`/`Bmf.Inbound` (success and `error.type`-bearing failure), assert the convention `DisplayName`/kind, and assert `AddBmfInstrumentation` registers the expected sources/meters (a `TracerProviderBuilder`/`MeterProviderBuilder` round-trip). Update the existing outbound diagnostics tests and the 0049 inbound tests to the new names/units.

## References

The implementation targets a single pinned semantic-conventions release, recorded here and in `MessagingSemanticConventions`. Pinned version: OpenTelemetry Semantic Conventions **v1.42.0**:

- Messaging spans — https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/
- Messaging metrics — https://opentelemetry.io/docs/specs/semconv/messaging/messaging-metrics/
- RabbitMQ messaging attributes — https://opentelemetry.io/docs/specs/semconv/messaging/rabbitmq/
- Versioned source (pinned tag) — https://github.com/open-telemetry/semantic-conventions/tree/v1.42.0/docs/messaging
- `error.type` — https://opentelemetry.io/docs/specs/semconv/attributes-registry/error/

## Implementation Notes

Resolutions for the ambiguities the plan flagged, confirmed against the pinned **v1.42.0** spec:

- **Span-name token order.** v1.42.0 fixes the messaging span name as `{messaging.operation.name} {destination}`, so the `DisplayName` is `send {exchange}` (producer) and `process {queue}` (consumer) — operation first, then destination. This is the opposite token order from the plan's illustrative `orders.created send`; the pinned spec wins, as the plan directed.
- **`messaging.operation.name` value.** Set to `send` (outbound) and `process` (inbound), matching `messaging.operation.type`, per the plan's attribute table.
- **Metric dimensions.** The `messaging.client.*` instruments carry only `messaging.system`, `messaging.operation.name`, `messaging.destination.name`, and (on failure) `error.type` — the low-cardinality common attributes of the metrics convention. `messaging.operation.type`, `messaging.message.id`, `messaging.message.body.size`, and the routing key are span-only.
- **Package versions.** `OpenTelemetry.Api` `1.16.0` (the integration package's only reference, added to `Directory.Packages.props`); the new `Bmf.OpenTelemetry.Tests` project additionally uses `OpenTelemetry`/`OpenTelemetry.Exporter.InMemory` `1.16.0` for the provider round-trip.
- **Provisioning tags.** `RabbitMqTopologyProvisioner` kept the out-of-scope `bmf.outbound.topology.provisioning.*` instruments and moved its two tag-name literals (`bmf.outbound.transport.name`, `bmf.outbound.outcome`) to local constants, since the shared `*TagName` constants were removed from `OutboundDiagnostics`.
