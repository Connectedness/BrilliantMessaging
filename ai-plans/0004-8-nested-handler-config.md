# Per-Handler Nested Configuration (Order-Insensitive Consumer Builder)

## Rationale

[0004-7](0004-7-consumer-unit-restructuring.md) made the consumer unit `(channel group, queue)` and moved the queue-scoped knobs (`PrefetchCount`, `Concurrency`, `ChannelCount`, `UseChannelGroup`, `UseInspector`, `ZeroCopyBody`) onto single consumer-level fields applied at `Build()`. That closed the original review concern for those knobs: interleaving them between `Handle` calls can no longer diverge, and a queue can no longer be configured inconsistently.

Two knobs were left behind as **sticky per-handler defaults**: `WithDeserializer` and `WithAckMode`/`ManualAck`. They set builder fields (`_deserializerType`/`_ackMode` in `RabbitMqInboundConsumerBuilder.cs:16-17`) that are snapshotted into each `RabbitMqInboundHandlerDefinition` at `Handle` time (`RabbitMqInboundConsumerBuilder.cs:151-160`). This reintroduces, in narrowed form, exactly the order-sensitivity the original reviewer flagged:

- `e.Handle<A, HA>().WithDeserializer<D>()` silently gives `A` the **default** deserializer; `D` applies to nothing.
- `e.Handle<A, HA>().WithAckMode(Manual).Handle<B, HB>()` gives `A` = Auto, `B` = Manual — a surprise for anyone who (reasonably, after 0004-7) assumes the fluent knobs are consumer-scoped and order-independent.

The asymmetry itself is the footgun: in one fluent chain, most methods are consumer-scoped and order-independent while two are sticky and order-dependent. This plan removes the asymmetry by taking the reviewer's second proposed fix — **move the genuinely per-handler knobs onto a per-`Handle` nested configuration** — so a per-handler setting is positionally bound to its handler and cannot be misplaced. The consumer builder surface becomes uniformly order-independent. This mirrors the established outbound pattern where per-target settings live on the nested `RabbitMqOutboundTargetBuilder<TMessage>` passed to `PublishNamed`. The library is pre-1.0, so the breaking change to the `Handle`/`HandleNamed` signatures is acceptable.

## Acceptance Criteria

- [x] `WithDeserializer<TDeserializer>`, `WithAckMode(MessageAckMode)`, and `ManualAck()` are **removed** from `RabbitMqInboundConsumerBuilder`, along with the now-unused `_deserializerType`/`_ackMode` consumer-level fields. The consumer builder retains only queue-scoped knobs (`PrefetchCount`, `Concurrency`, `ChannelCount`, `UseChannelGroup`, `UseInspector`, `ZeroCopyBody`) plus `Handle`/`HandleNamed`.
- [x] A new nested per-handler builder (`RabbitMqInboundHandlerBuilder`) exposes `WithDeserializer<TDeserializer>()`, `WithAckMode(MessageAckMode)`, and `ManualAck()` with the same validation and defaults as before (default deserializer `PayloadCodecMessageDeserializer`, default ack mode `Auto`).
- [x] `Handle<TMessage, THandler>(Action<RabbitMqInboundHandlerBuilder>? configure = null)` and `HandleNamed<TMessage, THandler>(string? endpointName, Action<RabbitMqInboundHandlerBuilder>? configure = null)` accept an optional per-handler configuration lambda. The terse case `Handle<A, HA>()` is unchanged; per-handler settings are expressed as `Handle<A, HA>(h => h.WithDeserializer<D>().ManualAck())`.
- [x] Per-handler settings can no longer be placed after the `Handle` they were meant for, nor leak onto a later handler: each handler's deserializer and ack mode are determined solely by its own configuration lambda. The whole consumer builder surface is order-independent.
- [x] `RabbitMqInboundHandlerDefinition` (shape and consumer descriptor) is unchanged — `DeserializerType` and `AckMode` still live per handler; only the *path by which they are set* moves from sticky consumer fields to the nested builder.
- [x] Existing unit and integration tests that set `WithDeserializer`/`WithAckMode`/`ManualAck` at the consumer level are migrated into the per-`Handle` lambda; no behavioral change to the compiled topology.
- [x] New unit tests cover: a per-handler deserializer/ack mode applied via the lambda; two handlers on one queue with **different** deserializers and ack modes set independently; and that the terse `Handle<A, HA>()` retains the defaults.

## Technical Details

### Nested handler builder (Usf.Transport.RabbitMq)

Introduce a non-generic `RabbitMqInboundHandlerBuilder` (the per-handler knobs do not depend on `TMessage`/`THandler`, unlike outbound where routing factories do, so no type parameters are needed):

```csharp
public sealed class RabbitMqInboundHandlerBuilder
{
    private Type _deserializerType = typeof(PayloadCodecMessageDeserializer);
    private MessageAckMode _ackMode = MessageAckMode.Auto;

    public RabbitMqInboundHandlerBuilder WithDeserializer<TDeserializer>()
        where TDeserializer : class, IMessageDeserializer { _deserializerType = typeof(TDeserializer); return this; }

    public RabbitMqInboundHandlerBuilder WithAckMode(MessageAckMode ackMode) { /* Enum.IsDefined guard */ return this; }

    public RabbitMqInboundHandlerBuilder ManualAck() => WithAckMode(MessageAckMode.Manual);

    internal (Type DeserializerType, MessageAckMode AckMode) Build() => (_deserializerType, _ackMode);
}
```

The `WithAckMode` `Enum.IsDefined` guard and the `IMessageDeserializer` constraint move verbatim from the current `RabbitMqInboundConsumerBuilder` (`:84-105`).

### Consumer builder (Usf.Transport.RabbitMq)

In `RabbitMqInboundConsumerBuilder`:

- Delete `WithDeserializer`, `WithAckMode`, `ManualAck` and the `_deserializerType`/`_ackMode` fields (`:16-17`, `:84-105`).
- Change `Handle`/`HandleNamed` to take the optional `Action<RabbitMqInboundHandlerBuilder>? configure`. `HandleNamed` constructs a `RabbitMqInboundHandlerBuilder`, invokes `configure?.Invoke(handlerBuilder)`, calls `Build()`, and passes the resulting `DeserializerType`/`AckMode` into the `RabbitMqInboundHandlerDefinition` (replacing the snapshot of the consumer fields at `:156`/`:158`). The concrete-type guard (`:143-149`) and `MessageHandlerInvocation.Create<TMessage, THandler>()` are unchanged.
- `Build()` (`:164-176`) no longer reads `_deserializerType`/`_ackMode`; the rest of `RabbitMqInboundConsumerDefinition` construction is unchanged.
- Update the `<summary>` XML docs on `Handle`/`HandleNamed` (`:122-139`) to document the new `configure` parameter and that per-handler deserializer/ack-mode settings are expressed through it.

`Handle` delegates to `HandleNamed(endpointName: null, configure)` as today.

### Compiler, topology, runtime

No changes. `RabbitMqInboundHandlerDefinition` keeps `DeserializerType`/`AckMode`, so `CompileInbound`, the per-handler endpoint build, the `(queue, discriminator)` dispatch index, and the runtime all see the identical compiled shape. This change is confined to the builder surface.

### Tests

- Migrate consumer-level `WithDeserializer` usages into the `Handle` lambda:
  - `AddRabbitMqConsumeTopologyTests.cs:347` and `:381` → `Handle<ValidationMessageA, ValidationMessageAHandler>(h => h.WithDeserializer<RawDeserializer>())`.
  - `RabbitMqDedicatedTopologiesIntegrationTests.cs:210` and `:217` → fold `WithDeserializer<…>` into the respective single `Handle` lambda (both consumers have one handler, so the migration is mechanical).
- New unit tests in `AddRabbitMqConsumeTopologyTests.cs`:
  - Per-handler deserializer and ack mode set via the lambda land on the compiled handler/endpoint.
  - Two handlers on one queue with different deserializers and different ack modes are configured independently and compile to distinct per-endpoint settings under one consumer.
  - `Handle<A, HA>()` with no lambda retains `PayloadCodecMessageDeserializer` and `MessageAckMode.Auto`.

### Considered and deferred

- **A typed `RabbitMqInboundHandlerBuilder<TMessage, THandler>`.** Unnecessary: neither per-handler knob depends on the message or handler type, so a non-generic builder keeps the surface minimal. Revisit only if a future per-handler knob needs the type (e.g. a per-handler typed filter).
- **Keeping `WithDeserializer`/`WithAckMode` on the consumer as sticky shortcuts.** Rejected: retaining them alongside the lambda would preserve the exact asymmetry/footgun this plan removes.
