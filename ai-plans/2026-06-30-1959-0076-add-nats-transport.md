# Add NATS Transport

## Rationale

BrilliantMessaging should gain a NATS transport as the next real broker transport after RabbitMQ. The initial transport should target JetStream only, because sagas and workflows require durable publish, acknowledgement, redelivery, retry, and dead-letter semantics that core NATS pub/sub cannot provide.

Core NATS support may be added later as a separate no-guarantees mode for notifications or other messages that do not need workflow-grade delivery.

This transport is delivered as a single package and a single pull request. Although the surface is broad, it is cohesive enough to land together; it is deliberately not split into sub-plans the way the RabbitMQ transport was.

## Acceptance Criteria

- [x] A NATS transport package is added to the solution for JetStream-backed messaging.
- [x] The package is named `BrilliantMessaging.Transport.Nats` and exposes `AddNatsTopology`, `AddNatsOutboundTopology`, and `AddNatsInboundTopology` entry points.
- [x] The public API and documentation make clear that this plan implements JetStream-backed NATS messaging only, while core NATS pub/sub is out of scope.
- [x] The transport exposes direction-specific `INatsOutboundTopologyBuilder` and `INatsInboundTopologyBuilder` interfaces so publish-only and consume-only registrations only expose the relevant configuration surface.
- [x] The transport exposes connection configuration (server URI and the credentials/options needed by `NATS.Net`) on the shared resource configuration, so applications and tests can target their own NATS endpoint.
- [x] `AddNatsInboundTopology` uses a default inbound topology name that does not collide with the default outbound topology name.
- [x] Outbound messages can be published to explicit NATS subjects via `ToSubject(...)`.
- [x] Outbound configuration supports named targets and target-level serializer overrides, matching the existing real broker transport pattern.
- [x] Streams can be declared with explicit subject patterns, and inbound consumers can bind durable JetStream consumers to streams and optional filter subjects.
- [x] Inbound configuration supports the existing pipeline customization, deserialization middleware replacement, named handlers, manual acknowledgement, handler-level deserializer overrides, and handler-level acknowledgement options. Any of these parity features that JetStream genuinely cannot support is documented as unsupported with the reason rather than left unaddressed.
- [x] JetStream publishing awaits server acknowledgement.
- [x] Outbound targets can opt into JetStream deduplication (default off); when enabled, the target sets the NATS `Nats-Msg-Id` header to the CloudEvents `id`, and the documentation states that effective once-only delivery also requires a stream duplicate window covering the subject.
- [x] The transport uses CloudEvents binary content mode over NATS headers.
- [x] Published and consumed messages flow through the normal BrilliantMessaging serialization, CloudEvents metadata, trace context, inbound inspection, deserialization middleware, acknowledgement middleware, and diagnostics path.
- [x] Successful handler completion acknowledges the JetStream message.
- [x] Handler failure triggers JetStream negative acknowledgement (with delay where supported) so the message is redelivered per the retry/backoff policy and delivery count.
- [x] `RetryMessageException` maps to a delayed negative acknowledgement consistent with the existing transport semantics.
- [x] `RejectMessageException` routes the message to the configured dead-letter destination and settles the original without further redelivery.
- [x] Retry exhaustion routes the message to the configured dead-letter destination.
- [x] Dead-letter routing republishes to the configured dead-letter subject or stream, awaits the publish acknowledgement, and only then settles the original message, consistent with the RabbitMQ and in-memory transports.
- [x] Long-running handlers are kept in-flight automatically: the transport periodically sends JetStream `AckProgress` for in-flight messages (on by default, opt-out), so a handler slower than `AckWait` is not redelivered prematurely.
- [x] Stream and consumer policy knobs (`AckWait`, `MaxDeliver`, `MaxAckPending`, storage, retention, replicas, and duplicate window) are configurable on the topology builder.
- [x] Stream and consumer topology can be provisioned idempotently by the framework, with an assert-only mode for externally managed JetStream infrastructure.
- [x] Topology validation catches duplicate streams or consumers, invalid subjects, missing stream references, consumers without handlers, dead-letter subjects not covered by a stream, invalid policy values, and incompatible assert-only topology.
- [x] `README.md` gains a short NATS transport section with a simple configuration example, in the same style as the in-memory transport section.
- [x] A dedicated NATS transport documentation page under `docs` covers setup, configuration, topology examples, JetStream reliability semantics, deduplication, retry and dead-letter behavior, ordering and long-running handler caveats, the default NATS maximum message size, reliance on `NATS.Net`'s built-in reconnection, and the Core NATS non-goal.
- [x] Unit tests cover builder validation, topology compilation, acknowledgement mapping, and header/body translation.
- [x] Integration tests use `Testcontainers.Nats` with JetStream enabled and cover publish/consume, stream and durable consumer provisioning, publish acknowledgement, retry/redelivery, and dead-letter republish behavior.
- [x] Release builds stay warning-clean with `TreatWarningsAsErrors`.

## Technical Details

Add a new transport project under `src/Transports/BrilliantMessaging.Transport.Nats` and a matching test project under `tests/Transports/BrilliantMessaging.Transport.Nats.Tests`. Reference Synadia's current `NATS.Net` metapackage, which brings in `NATS.Client.Core` and `NATS.Client.JetStream` transitively, rather than the legacy `NATS.Client` package. The repo uses central package management, so register the direct references (`NATS.Net` and, for the tests, `Testcontainers.Nats`) in `Directory.Packages.props`; the transitive `NATS.Client.*` packages do not need their own entries. `NATS.Net` (2.8.2) supports `netstandard2.0`, matching the existing transports' target framework.

Expose the transport as a general NATS package because core NATS and JetStream share the same NATS server and client connection. The public entry points should follow the existing transport style: `NatsTransportModule`, `AddNatsTopology`, `AddNatsOutboundTopology`, and `AddNatsInboundTopology`. The implemented topology concepts are JetStream-only: streams, durable consumers, filter subjects, and JetStream acknowledgement policies. Documentation and XML comments should state that core NATS pub/sub is not implemented by this plan.

Use direction-specific builder interfaces, following the RabbitMQ and in-memory transport pattern. `INatsOutboundTopologyBuilder` should expose publishing configuration and shared NATS resource configuration but no consumers. `INatsInboundTopologyBuilder` should expose streams, durable consumers, inbound pipeline customization, deserialization middleware replacement, shutdown timeout, and shared NATS resource configuration but no publishing targets. The parameterless `AddNatsInboundTopology` overload should use a NATS default inbound topology name, such as `NatsTransportModule.DefaultInboundName`, so it can be paired with a default outbound topology without a topology-name collision.

Model NATS subjects as the routing surface. Subject mapping is explicit in the MVP: outbound targets use `ToSubject("orders.placed")`, streams declare subject patterns, and consumers use `FilterSubject("orders.placed")` when needed. Publish subjects passed to `ToSubject(...)` must be literal, while stream subject patterns may use NATS wildcards (`*` and `>`); validation enforces this distinction rather than treating all subjects alike. Convention-based subject derivation is out of scope for this plan. There is no exchange/routing-key split in NATS. Outbound targets should support both `Publish<TMessage>(...)` and `PublishNamed<TMessage>(...)`; target-level serializer overrides are supported in the same style as RabbitMQ (serialization happens before the NATS client path, so this is always achievable). Outbound targets also expose an opt-in deduplication switch, such as `UseMessageIdDeduplication()`, which is off by default; when enabled, the publish path sets the NATS `Nats-Msg-Id`
header to the CloudEvents `id`.

Model JetStream streams and durable consumers as explicit topology resources. A stream owns one or more subject patterns, and an inbound consumer references the stream, durable consumer name, optional filter subject, concurrency, failure policy, and an optional dead-letter destination declared on the builder (for example `DeadLetterSubject("orders.placed.dead")`). Durable consumer names are operationally significant because changing the name creates a distinct server-side consumer.

Compile NATS topology into regular `OutboundTarget` and `InboundEndpoint` instances so the existing `MessagePublisher`, serializers, CloudEvents metadata, W3C trace context propagation, inbound inspectors, deserialization middleware, framework acknowledgement middleware, and diagnostics remain in the path. The transport should construct NATS-specific `TransportMessage` and acknowledgement adapters rather than bypassing the shared BrilliantMessaging pipeline.

Use pull-based durable consumers with explicit acknowledgement, and map each outcome to the matching JetStream settlement primitive rather than settling generically: successful processing sends `Ack`; retryable failures (including `RetryMessageException`) send `Nak` with a delay derived from the existing retry/backoff policy and delivery count; rejected messages (`RejectMessageException`) and retry exhaustion first synchronously republish to the configured dead-letter subject or stream, await the publish acknowledgement, and then send `Term` on the original so it is removed without further redelivery. This keeps dead-letter behavior aligned with the RabbitMQ and in-memory transports while using the correct JetStream primitive for each case. Inbound consumer and handler configuration should reach feature parity with RabbitMQ: `ConfigureInboundPipeline`, `UseDeserializationMiddleware<TMiddleware>()`, `HandleNamed`, `ManualAck`, handler-level deserializer overrides, and handler-level
acknowledgement mode selection. Any of these that JetStream genuinely cannot support is documented as unsupported with the reason rather than silently dropped.

The topology provisioner should mirror the RabbitMQ direction: framework-owned active provisioning by default, idempotent create/update where safe, clear failure on incompatible existing resources, and a passive/assert-only mode for teams that manage streams and consumers with external tooling. Policy knobs should include the JetStream equivalents needed by the transport, such as `AckWait`, `MaxDeliver`, `MaxAckPending`, storage, retention, replicas, and duplicate window where applicable.

Topology compilation and provisioning should validate duplicate streams or consumers, invalid or blank subjects (including wildcards used in a literal publish subject), consumers that reference missing streams, consumers without handlers, dead-letter subjects not covered by a declared stream, invalid policy values, and assert-only mode mismatches with existing server topology.

Document the reliability contract as at-least-once consume with idempotent handlers expected. Publish can be effectively once when deduplication is enabled on the outbound target (which sets the CloudEvents id as the NATS message id) and the capturing stream declares a duplicate window covering the subject; deduplication is off by default, so the NATS transport documentation page must spell out both prerequisites. Ordering is not guaranteed once retries, delayed redelivery, or concurrent consumers are involved. Long-running handlers are kept in-flight by the transport automatically sending JetStream `AckProgress` (`+WPI`) heartbeats for in-flight messages on a timer derived from `AckWait`; this is on by default and can be opted out, in which case operators must size `AckWait` to cover the slowest handler.

Integration tests should use `Testcontainers.Nats` to run a NATS server with JetStream enabled. The default `Testcontainers.Nats` container does not enable JetStream, so the container must launch NATS with the `-js` server flag (or equivalent configuration). The transport should expose connection configuration for server URI and the credentials/options needed by `NATS.Net`, so tests and applications can provide their own NATS endpoint. Connection resilience relies on `NATS.Net`'s built-in reconnection rather than a bespoke reconnect layer like the RabbitMQ transport needed.

Documentation should add a short README section that mirrors the in-memory transport section — a brief description plus a simple configuration example — and keep the real documentation in a dedicated NATS transport page under `docs`. That page covers package installation, a minimal configuration example, stream and durable consumer topology, publish and consume examples, connection configuration, JetStream acknowledgement and redelivery semantics, deduplication, retry and dead-letter behavior, ordering caveats, long-running handler caveats, the default NATS maximum message size (1 MB by default), reliance on `NATS.Net`'s built-in reconnection, and the explicit non-goal of core NATS pub/sub support in this plan. The `Testcontainers.Nats` package is a test-only concern and does not need to be documented.

This plan does not add core NATS publishing or a volatile pub/sub capability model. The general NATS package and entrypoint names leave room for future core NATS support, while all implemented topology resources and reliability semantics in this plan are JetStream-backed.
