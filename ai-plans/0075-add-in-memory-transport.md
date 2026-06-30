# Add In-Memory Transport

## Rationale

BrilliantMessaging should gain a supported in-memory transport before adding another real broker transport such as NATS. The transport gives tests, samples, local development, and saga/workflow experimentation a broker-free runtime while still exercising the real BrilliantMessaging publish, serialization, CloudEvents, inbound pipeline, acknowledgement, diagnostics, and shutdown behavior.

The transport is intentionally process-local and non-durable. It should be useful and supported, but it must not pretend to provide distributed broker semantics.

## Acceptance Criteria

- [x] A new `BrilliantMessaging.Transport.InMemory` transport package is added to the solution and wired into `BrilliantMessaging.slnx`.
- [x] Public APIs are XML-documented and the project follows the repository's explicit `using` convention.
- [x] The user-facing API uses the agreed naming: `AddInMemoryTopology`, `AddInMemoryOutboundTopology`, `AddInMemoryInboundTopology`, `Topic`, `ToTopic`, `Consume`, `Concurrency`, `OnFailure`, `Retry`, `MaxAttempts`, `LinearBackoff`, `ExponentialBackoff`, and `DeadLetterTo`. `Topic` explicitly declares a topic resource; `ToTopic` and `Consume` reference a declared topic.
- [x] Published messages are dispatched through background queues, not inline during publish.
- [x] The transport supports topic-based publish/consume routing, with fanout to each configured consumer route for a topic.
- [x] Each consumer route preserves single-worker FIFO delivery by default and supports configurable concurrency.
- [x] The default handler failure behavior drops the failed delivery.
- [x] Per-consumer delivery policy configuration supports retry with max attempts and linear or exponential backoff.
- [x] Retry backoff scheduling uses an injectable time/scheduler abstraction so automated tests can drive delayed retries deterministically without sleeping.
- [x] `MaxAttempts` counts total delivery attempts, including the initial delivery; when attempts are exhausted, the delivery is republished to the topic named by `DeadLetterTo`, or dropped if `DeadLetterTo` is not configured.
- [x] `RetryMessageException` requests another attempt only when an explicit `Retry(...)` policy is configured, and then respects the configured retry/backoff policy; otherwise the default drop policy applies.
- [x] `RejectMessageException` republishes the delivery to the topic named by `DeadLetterTo`, or drops it if `DeadLetterTo` is not configured.
- [x] The transport uses real serialization and the normal CloudEvents, inspection, deserialization, middleware, acknowledgement, and diagnostics path.
- [x] An explicit public `InMemoryBroker` support service records every message routed to any topic and exposes `GetMessages(string topic)` so tests can assert what reached a topic (including its dead-letter topic); dead-letter inspection is just inspecting the configured `DeadLetterTo` topic.
- [x] The explicit public `InMemoryBroker` support service exposes `DrainUntilIdleAsync(TimeSpan timeout, CancellationToken cancellationToken = default)`.
- [x] `DrainUntilIdleAsync` waits for queued deliveries, in-flight handlers, and already scheduled retry deliveries to finish until idle, timeout, or cancellation.
- [x] In-memory broker state is registered as a singleton, keyed by topology where needed, so state is isolated per service provider by default.
- [x] Shutdown stops accepting new work, drains in-flight work until the configured timeout, then cancels remaining work.
- [x] `README.md` is updated with a short in-memory transport section that links to comprehensive documentation under `./docs`, including a concise example and the transport's process-local, non-durable semantics.
- [x] Automated tests need to be written, following the repository test rules.
- [x] Release builds stay warning-clean with `TreatWarningsAsErrors`.

## Technical Details

Add a new transport project under `src/Transports/BrilliantMessaging.Transport.InMemory` and a matching test project under `tests/Transports/BrilliantMessaging.Transport.InMemory.Tests`. Wire both into `BrilliantMessaging.slnx` and follow the existing RabbitMQ project structure where it helps, without copying RabbitMQ broker concepts. Follow the existing transport project conventions for `Directory.Build.props`, central package versions, nullable/warnings settings, solution folder placement, and test runner configuration.

The public builder surface should mirror the existing transport pattern: `InMemoryTransportModule`, `InMemoryTopologyBuilder`, direction-specific builder interfaces if needed, `InMemoryOutboundTargetBuilder<TMessage>`, and `InMemoryInboundConsumerBuilder`. A topic is the only resource concept in the MVP and is declared explicitly via `Topic("orders")`. `Publish<TMessage>(target => target.ToTopic("orders"))` maps outbound messages to a declared topic, and `Consume("orders", consumer => consumer.Handle<TMessage, THandler>())` maps inbound handlers from a declared topic. The declared topic is also the key under which per-topic message inspection is exposed. Multiple consumers on the same topic receive fanout deliveries; competing consumer groups are out of scope for the MVP.

Compile the topology into regular `OutboundTarget` and `InboundEndpoint` instances so existing `MessagePublisher`, serializers, CloudEvents metadata, inbound inspectors, middleware, acknowledgement behavior, and diagnostics remain in the path. The in-memory transport should construct concrete `TransportMessage` instances that carry CloudEvents attributes in headers in the same spirit as the RabbitMQ binary-mode binding.

Runtime state should live in an in-memory broker registered as a singleton, keyed by topology where needed, so the default registration isolates state per service provider while sharing state across that provider's publishers, runtimes, and support APIs. The design may allow an explicitly supplied shared broker instance later, but that is not required for the first implementation.

Dispatch should enqueue serialized deliveries onto per-consumer-route background workers. Default concurrency is `1`, giving FIFO behavior for that route. Increasing `Concurrency(n)` allows parallel processing and relaxes strict ordering for that route. A topology runtime should own worker lifecycle and follow the existing `ITopologyRuntime` start/stop model.

Delivery policy is configured on the inbound consumer route via `OnFailure`. Default policy is drop. Retry policy records `MaxAttempts` and a backoff strategy: none/immediate, linear, or exponential. `MaxAttempts` counts total delivery attempts, including the initial delivery. Handler failures, `RetryMessageException`, and `RejectMessageException` must be mapped consistently: ordinary failures follow the configured policy, retry exceptions request retry only when an explicit `Retry(...)` policy is configured, and reject exceptions go to dead-letter handling. Without an explicit `Retry(...)` policy, `RetryMessageException` behaves like any other failure under the default drop policy and is dropped. Retry scheduling should go through an injectable time/scheduler abstraction; the default runtime uses real time, while tests can supply a fake or manual scheduler. Dead-lettering is plain republishing: `DeadLetterTo("topic")` republishes the failed or exhausted delivery to the named topic, and when `DeadLetterTo` is not configured the delivery is dropped. There is no separate dead-letter store; dead-lettered messages are inspected through the general per-topic inspection API on the dead-letter topic.

The in-memory acknowledgement implementation is the adapter between the existing inbound pipeline and the delivery policy. `AckAsync` completes the delivery. `NackAsync(requeue: true)` schedules another attempt through the configured retry/backoff policy. `NackAsync(requeue: false)` drops the delivery or routes it to dead-letter handling. This keeps `FrameworkMessageAcknowledgementMiddleware`, `RedeliveryClassifier`, `RetryMessageException`, and `RejectMessageException` in the normal path instead of bypassing them.

Expose explicit support APIs for tests, not hidden internals. The first useful hooks live on a public `InMemoryBroker` support service: `GetMessages(string topic)` returns the recorded messages for a declared topic, and `DrainUntilIdleAsync(TimeSpan timeout, CancellationToken cancellationToken = default)` drains a topology/broker until idle. Per-topic inspection records every message routed to a topic (an observation log keyed by declared topic, including messages that were consumed), so tests can assert that a message reached a given topic even when that topic has consumers; dead-letter inspection is simply inspecting the `DeadLetterTo` topic. The recorded message type should be public, for example `InMemoryTransportMessage`, and carry the serialized body and headers needed for assertions. This recording is a test/support facility for the process-local transport and must not be relied on by the normal runtime path. Drain-until-idle should observe queued work, active handler invocations, and scheduled retry timers that already exist when the drain starts or are created by work being drained. It must accept timeout/cancellation so tests cannot hang indefinitely. These APIs should not compromise the normal runtime path.

Shutdown should stop accepting new queued work for the runtime, allow in-flight deliveries to complete until the configured shutdown timeout, and then cancel remaining work. The shutdown timeout is a topology-level setting on the in-memory topology, mirroring `RabbitMqTopology.ShutdownTimeout`; the runtime is driven through the existing `TopologyRuntimeHostedService`/`ITopologyRuntime` start/stop model and drains in-flight deliveries against that timeout before cancelling, rather than introducing a new host-level option. Publish attempts after the runtime has stopped should fail clearly.

Documentation should keep `README.md` concise: add a short in-memory transport section with a minimal configuration example, the process-local and non-durable caveats, and a link to comprehensive in-memory transport documentation under `./docs`. The `./docs` page should cover the full builder API, routing semantics, retry/dead-letter behavior, support APIs, drain behavior, and shutdown behavior.

NATS, cross-process communication, durability, persistence, competing consumer groups, delayed scheduled delivery beyond retry backoff, and distributed broker fidelity are out of scope for this plan.
