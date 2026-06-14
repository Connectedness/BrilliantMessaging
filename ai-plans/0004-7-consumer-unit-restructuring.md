# Consumer-Unit Restructuring (Queue-Scoped Consumers)

## Rationale

The inbound runtime makes the **endpoint** the unit of consumption, but multiple endpoints legally map to one queue — the canonical case being a queue fed by several event types (`order.*` bound to one queue) dispatched to one `IMessageHandler<T>` each. The `(queue, discriminator)` dispatch index exists precisely for that case, yet the surrounding machinery treats each `Handle<T,H>()` as an independent consumer:

- Each `Handle` becomes its own endpoint, and an endpoint with no explicit channel group gets a *fresh* implicit channel group (`RabbitMqTopologyCompiler.cs:477-484`). So `Consume("q", e => e.Handle<A,…>().Handle<B,…>())` produces two endpoints in two channel groups.
- `RabbitMqTopologyRuntime.StartAsync` opens one channel per group and issues `BasicConsume` **per endpoint** (`RabbitMqTopologyRuntime.cs:79-96`). Two same-queue endpoints become two competing consumers on the same queue. Because `BasicQos` uses `global: false` (per-consumer prefetch), the queue's effective prefetch window silently **doubles**.
- The inspector that runs is the *subscribed* endpoint's (`RabbitMqTopologyRuntime.cs:238-239`), chosen by which consumer the broker round-robined the delivery to — then the *dispatched* endpoint is re-resolved from the index. Routing stays correct, but if same-queue endpoints declare different inspectors, the inspector is **nondeterministic per delivery**. [0004-0](0004-0-message-consumers.md) specifies the inspector as "per queue"; the implementation made it per endpoint.

The fix is to make the consumer unit **`(channel group, queue)`** instead of the endpoint: one `BasicConsume` per channel per distinct queue, with the inspector, copy-body mode, and channel-group assignment owned by the *queue* (the consumer), and endpoints reduced to pure `discriminator → handler` dispatch targets selected after inspection. Same-queue competing consumers then arise only from an explicit `ChannelCount > 1` (intentional parallelism), never by accident. The library is pre-1.0, so the breaking changes to the inbound definition/builder/configuration types are acceptable.

## Acceptance Criteria

- [ ] The inbound configuration carries a per-queue **consumer** definition instead of a flat handler list: queue-scoped settings (queue name, inspector, channel-group reference, channel count, prefetch, dispatch concurrency, copy-body) live on the consumer; per-handler settings (message type, handler type, handler invocation, deserializer, ack mode, endpoint name) live on the handlers nested under it.
- [ ] `Consume(queue, configure)` produces exactly one consumer definition. The queue-scoped knobs (`UseInspector`, `UseChannelGroup`, `ChannelCount`, `PrefetchCount`, `Concurrency`, `ZeroCopyBody`) apply to the whole consumer regardless of call order; `Handle`/`HandleNamed` (with their per-handler `WithDeserializer`/`WithAckMode`) only add dispatch entries. The queue-scoped settings can no longer be expressed inconsistently across the handlers of one queue.
- [ ] The compiler builds one compiled consumer per queue (resolving its channel group), and one endpoint per handler under it. The `(queue, discriminator)` dispatch index and the by-endpoint-name lookup are unchanged. An implicit channel group is synthesized **per consumer (queue)**, not per endpoint, so same-queue endpoints share one channel group.
- [ ] The runtime opens `MaximumChannelCount` channels per channel group and issues **one `BasicConsume` per channel per queue** assigned to that group, carrying the queue's inspector and copy-body mode in the consumer closure. Two endpoints on one queue (default settings) produce exactly one channel and one consumer; prefetch is no longer doubled.
- [ ] `ProcessDeliveryAsync` resolves the inspector and builds the `TransportMessage` from the **consumer** (queue + copy-body), then dispatches through the existing `TryGetEndpoint(queue, discriminator)` lookup and type-validation. No per-endpoint inspector selection remains.
- [ ] The compiler rejects a queue configured by more than one `Consume(...)` call (ambiguous queue-scoped settings) and rejects a `Consume(queue)` that declares no handlers (an endpoint-less consumer would subscribe only to NACK every delivery to the DLX). The now-structurally-guaranteed per-queue copy-body agreement check (`RabbitMqTopologyCompiler.cs:1080-1088`) is removed as redundant.
- [ ] `ChannelCount > 1` on a consumer yields N competing consumers on that one queue (intentional parallelism); the channel-budget calculation reflects the coalesced channel groups.
- [ ] Automated tests (unit + RabbitMQ integration) are written, and existing inbound tests are updated to the new shape.

## Technical Details

### Definitions (Usf.Transport.RabbitMq)

Split `RabbitMqInboundHandlerDefinition` into a per-queue consumer record and a slimmed per-handler record:

```csharp
public sealed record RabbitMqInboundConsumerDefinition(
    string QueueName,
    Type InspectorType,
    string? ChannelGroupName,
    int ChannelCount,
    ushort PrefetchCount,
    ushort ConsumerDispatchConcurrency,
    bool CopyBody,
    IReadOnlyList<RabbitMqInboundHandlerDefinition> Handlers
);

public sealed record RabbitMqInboundHandlerDefinition(
    string? EndpointName,
    Type MessageType,
    Type HandlerType,
    MessageDelegate HandlerInvocation,
    Type DeserializerType,
    MessageAckMode AckMode
);
```

The queue-scoped fields (`InspectorType`, `ChannelGroupName`, `ChannelCount`, `PrefetchCount`, `ConsumerDispatchConcurrency`, `CopyBody`, `QueueName`) move off the handler and onto the consumer; `DeserializerType` and `AckMode` stay per-handler (legitimately per message type, per [0004-6](0004-6-CloudEvents-agnostic-consumer-pipeline.md)). `RabbitMqTopologyConfiguration.Handlers` becomes `Consumers : IReadOnlyList<RabbitMqInboundConsumerDefinition>` (`RabbitMqTopologyConfiguration.cs:23`); `HasInboundEndpoints` becomes `Consumers.Count > 0`.

### Builder (Usf.Transport.RabbitMq)

`RabbitMqInboundEndpointBuilder` is repurposed as the per-queue consumer builder (rename to `RabbitMqInboundConsumerBuilder` for clarity; it is the `Action<…>` argument of `Consume`). The queue-scoped knobs (`UseInspector`, `UseChannelGroup`, `ChannelCount`, `PrefetchCount`, `Concurrency`, `ZeroCopyBody`) set single consumer-level fields rather than being captured into each handler at `Handle` time, so interleaving them between `Handle` calls can no longer create per-handler divergence. `Handle`/`HandleNamed` append per-handler entries; `WithDeserializer`/`WithAckMode` remain sticky per-handler defaults. `Build()` returns one `RabbitMqInboundConsumerDefinition`. `RabbitMqTopologyBuilder.Consume` (`RabbitMqTopologyBuilder.cs:419-433`) adds that single definition to a `_consumers` list instead of `AddRange`-ing flattened handlers.

### Compiler (Usf.Transport.RabbitMq)

`CompileInbound` (`RabbitMqTopologyCompiler.cs:376-434`) iterates consumers, not handlers:

- Resolve the channel group per consumer. `ResolveInboundChannelGroup` (`:465-485`) keys the implicit group by **queue/consumer** (using the consumer's `ChannelCount`/`PrefetchCount`/`ConsumerDispatchConcurrency`), so all endpoints of a queue share one group. When a consumer references an **explicit** channel group via `UseChannelGroup`, that group's QoS (`MaximumChannelCount`/`PrefetchCount`/`ConsumerDispatchConcurrency`) governs and the per-consume knobs feed only the synthesized implicit group — matching the existing outbound channel-group precedence rather than introducing a "both set" validation.
- For each handler under the consumer, build the endpoint (`CreateEndpointCore`, `:502-523`) and add it to `endpointsByName` and to the `(queue, discriminator)` dispatch index exactly as today.
- Produce a compiled consumer descriptor (below) per consumer.

`RabbitMqInboundEndpoint` no longer needs `QueueName`, `InspectorType`, or `CopyBody` as per-endpoint state — those move to the consumer descriptor (Core's `InboundEndpoint` never carried them; they are RabbitMQ-specific). `RabbitMqInboundEndpoint.ChannelGroup` likewise moves to the consumer; keep only what dispatch/handler-invocation needs.

`ValidateHandlers` (`:1069-1153`) is restructured to validate per consumer:

- Reject a queue named by more than one `Consume` (one consumer per queue): a clear `"Queue 'q' is configured by multiple Consume(...) calls."` error.
- Reject a consumer with no handlers: `"Consume('q') declares no handlers."` — without this guard the restructure would, unlike today's flattened model, compile an endpoint-less consumer that subscribes and NACKs every delivery to the DLX.
- Drop the per-queue copy-body disagreement check (`:1080-1088`) — copy-body is now a single consumer field and cannot disagree.
- Keep the inspector-registered, deserializer-registered/typed, handler-registered, queue-exists, channel-group-exists, ack-mode, discriminator-uniqueness, and endpoint-name-uniqueness checks, re-homed to the consumer/handler split. `ValidateInboundChannelGroupUsage` reads channel-group references off consumers.

### Topology and runtime descriptor (Usf.Transport.RabbitMq)

Introduce a compiled `RabbitMqInboundConsumer` carrying `QueueName`, `InspectorType`, `CopyBody`, the `RabbitMqInboundChannelGroup`, and its endpoints. `RabbitMqTopology` holds `IReadOnlyList<RabbitMqInboundConsumer> Consumers` and exposes `ConsumersByChannelGroup` (group consumers by channel group) in place of `EndpointsByChannelGroup` (`RabbitMqTopology.cs:98-99`). `TryGetEndpoint` and the dispatch index are unchanged.

### Runtime (Usf.Transport.RabbitMq)

`StartAsync` (`RabbitMqTopologyRuntime.cs:59-98`) iterates `ConsumersByChannelGroup`: for each channel group open `MaximumChannelCount` channels with `BasicQos(prefetchCount)`, and for each channel issue **one `BasicConsume` per consumer (queue)** in the group, with `consumer.ReceivedAsync += (_, e) => OnReceivedAsync(rabbitMqConsumer, channel, e)`. The closure captures the `RabbitMqInboundConsumer`, not an endpoint.

`OnReceivedAsync`/`ProcessDeliveryAsync` (`:175-290`) take the consumer instead of `subscribedEndpoint`: build the `RabbitMqTransportMessage` from `consumer.QueueName` and `consumer.CopyBody`, resolve `consumer.InspectorType`, inspect, then `TryGetEndpoint(consumer.QueueName, inspectResult.Discriminator, out endpoint)`, run the existing type validation, build the `IncomingMessageContext`, and invoke the pipeline. Everything downstream of dispatch (context construction, ack/nack matrix, drain) is unchanged.

### Channel budget

`CalculateChannelBudget` (`:544-566`) already sums `MaximumChannelCount` across channel groups; it stays correct, but the group count now drops (same-queue endpoints coalesce), which is the intended effect. A channel group serving K queues opens `MaximumChannelCount` channels, each running K consumers (one per queue) — channels, not consumers, are the budgeted resource. Note that `BasicQos` keeps `global: false`, so prefetch remains *per consumer*: a shared channel hosting K co-located queues carries up to K × `PrefetchCount` unacked messages. That is expected for an opt-in shared group and is unrelated to the per-queue doubling this plan removes (which came from two consumers on the *same* queue).

### Tests

- Update `tests/Transports/Usf.Transport.RabbitMq.Tests/Unit/AddRabbitMqConsumeTopologyTests.cs` and `RabbitMqChannelGroupTests.cs` for the consumer/handler split and the removed per-handler queue knobs.
- New unit tests: two endpoints on one queue compile to one channel group / one consumer (no doubled prefetch); a duplicate `Consume` for one queue is a compile error; an empty `Consume` (no handlers) is a compile error; `ChannelCount > 1` yields N consumers on the queue; same-queue endpoints share one inspector by construction.
- Integration: a queue carrying two event types dispatches each to its handler over a single consumer; `ChannelCount`-based parallelism drains concurrently; the safe-default ack/nack matrix and graceful drain still hold.

### Heterogeneous message formats on one queue

A queue may legitimately carry mixed formats — e.g. CloudEvents deliveries alongside non-CloudEvents payloads (S3 notifications, third-party webhooks). The single-inspector-per-queue rule does **not** restrict this, because the inspector is the *classification* stage and a delivery has exactly one classification: two inspectors racing to classify the same delivery is the very nondeterminism this plan removes. Mixed formats are served by **one inspector that branches** on a cheap discriminating signal (CloudEvents binding headers / content-type present → classify as CloudEvents; otherwise sniff body/headers), producing distinct discriminators that route to distinct endpoints. The genuinely per-format concern — *decoding* — is already per-endpoint via the [0004-6](0004-6-CloudEvents-agnostic-consumer-pipeline.md) `DeserializerType`, so the CloudEvents discriminator routes to an endpoint with the default deserializer and the custom-format discriminator to an endpoint with its own `IMessageDeserializer`. No part of this scenario needs a second inspector on the queue.

### Considered and deferred

- **An ordered first-match inspector combinator.** A framework-provided `CompositeInboundMessageInspector` that runs a configured list of inspectors in order (first non-unknown classification wins) would let users compose `CloudEventsInboundMessageInspector` with a custom inspector for a mixed-format queue without hand-writing the branch above. It is purely additive (a custom inspector already covers the case) and deterministic only because the order is explicit, so it is left as an optional convenience outside this plan's scope.
- **Merging multiple `Consume` calls for one queue** instead of rejecting them. Rejecting keeps the queue-scoped settings unambiguous with no merge-conflict rules; a single `Consume` lambda already expresses any number of handlers.
- **Forbidding an explicit channel group from spanning queues with different inspectors.** Unnecessary: the inspector is now per consumer (queue) and captured in the per-queue `BasicConsume` closure, so co-located queues on a shared channel group each keep their own inspector deterministically.
