# CloudEvents-Agnostic Consumer Pipeline

## Rationale

The inbound topology is described as CloudEvents-agnostic — CloudEvents is supposed to be one inspector/deserializer pair among many, so non-CloudEvents payloads (S3 notifications, third-party webhooks relayed onto a queue, bespoke binary formats) are first-class. The implementation does not yet hold to that. Three seams leak CloudEvents into the neutral spine:

1. **`InboundMessageInspectionResult.Envelope` is hard-typed `CloudEventEnvelope`** (Core), and `RabbitMqTopologyRuntime.ProcessDeliveryAsync` special-cases it (`if (inspectResult.Envelope is { } envelope) context.SetItem(CloudEventsContextKeys.Envelope, envelope)`). A generic routing result carries a CloudEvents property, and the transport runtime knows about CloudEvents.
2. **`IMessageSerializer.DeserializeAsync` takes a `CloudEventEnvelope`** — but `IMessageSerializer` is fundamentally the *outbound* CloudEvents serializer (`SerializeAsync` returns an envelope; the same singleton backs outbound targets). Inbound deserialization was bolted on for symmetry, forcing every inbound deserializer to speak "envelope."
3. **`MessageDeserializationMiddleware` does `GetRequiredItem(CloudEventsContextKeys.Envelope)`** — so a custom inspector that does not set that item fails with the obscure *"Message context item 'cloudevents.envelope' is not set"*, and the only escape is to deserialize eagerly in the inspector, contradicting "deserialization is the first middleware."

The key observation that makes this cheap: `CloudEventMessageSerializer.DeserializeAsync` only ever touches `envelope.Data` (CloudEventMessageSerializer.cs:99), and `envelope.Data` **is** `transportMessage.Body` — the inspector passes the body straight through (CloudEventsInboundMessageInspector.cs:54). Every other envelope attribute (id/source/type/time) is parsed by the *inspector*, not the deserializer. USF only emits and reads CloudEvents **binary content mode**, so the message data is always the transport body, never a structured JSON wrapper. The envelope parameter on deserialize is therefore gratuitous: a deserializer needs only the body bytes plus the content metadata already promoted onto `TransportMessage`.

The target state:

- **Serialize and deserialize are split into two interfaces.** `IMessageSerializer` stays the outbound (today CloudEvents) side; a new **`IMessageDeserializer`** is the inbound side and is CloudEvents-agnostic — it takes the neutral `IncomingMessageContext` (reading `Transport`, `Services`, and any generic context items the inspector or a prior middleware contributed) plus the target type, never a CloudEvents type. (Decoupling *publishing* from CloudEvents — `IMessageSerializer` no longer being obligated to produce a `CloudEventEnvelope` — is a separate, later plan; this slice only splits the concern so that work has a clean seam.)
- **The default deserialization path is CloudEvents-free.** The default deserializer decodes `TransportMessage.Body` through the existing `IPayloadCodec`; the default deserialization middleware reads `context.Transport`, not a CloudEvents context item. CloudEvents becomes purely an *inspector* concern plus an *optional* handler-facing context item.
- **The inspector stays a pure router; the runtime is the context factory.** `InboundMessageInspectionResult` shrinks to routing output `(Discriminator, MessageType)` plus neutral, generic context contributions. The runtime resolves the endpoint (`RabbitMqTopology.TryGetEndpoint`), validates, then constructs an `IncomingMessageContext` whose identity is immutable, seeding the inspector's contributions. The inspector never sees the live context and never resolves endpoints.

This keeps the zero-config CloudEvents case unchanged (`Consume(q).Handle<T,H>()` still "just works") while making a raw consumer "a custom inspector + a custom deserializer" with no CloudEvents types touched and no obscure failure.

The library is pre-1.0, so the breaking changes to `IMessageSerializer`, `InboundMessageInspectionResult`, `IncomingMessageContext`, and the inbound endpoint/definition/builder are acceptable.

## Acceptance Criteria

- [ ] `IMessageSerializer` no longer declares `DeserializeAsync`; it is the outbound serialization seam only. A new `IMessageDeserializer` declares `ValueTask<object?> DeserializeAsync(IncomingMessageContext context, Type messageType, CancellationToken cancellationToken = default)` and carries no CloudEvents type in its signature; `messageType` stays explicit so the middleware owns the target-type decision while the context supplies `Transport`, `Services`, and items.
- [ ] A default `IMessageDeserializer` implementation deserializes by decoding `context.Transport.Body` through `IPayloadCodec`, wrapping codec failures in `MessageDeserializationException` exactly as the old `CloudEventMessageSerializer.DeserializeAsync` did. `CloudEventMessageSerializer` loses its `DeserializeAsync` member and is purely an `IMessageSerializer`.
- [ ] The strongly-typed item bag is extracted from `IncomingMessageContext` into a reusable `IncomingMessageContextItems` type owning `SetItem<T>`/`TryGetItem<T>`/`GetRequiredItem<T>`/`RemoveItem<T>` over a lazily-initialized `Dictionary<object, object?>`. It can be instantiated standalone (so an inspector can pre-seed one) and adopted by a context without copying.
- [ ] `InboundMessageInspectionResult` is an immutable `(string Discriminator, Type MessageType)` with `object? Message { get; init; }` and `IncomingMessageContextItems? Items { get; init; }`; an inspector pre-seeds the `Items` instance with strongly-typed, CloudEvents-free entries. The `Envelope` property and the `CloudEventEnvelope` constructor overload are removed, and there is no in-place fluent mutation of the result.
- [ ] `IInboundMessageInspector.InspectAsync` continues to take `TransportMessage` (not the live context) and returns the slimmed `InboundMessageInspectionResult`. `CloudEventsInboundMessageInspector` resolves the type and parses the envelope as today, then returns `Discriminator`/`MessageType` and a pre-seeded `Items` carrying the envelope under `CloudEventsContextKeys.Envelope`. It does not set `Message`.
- [ ] `IncomingMessageContext` exposes the bag as `Items` and `Endpoint`, `Transport`, `Services`, `Acknowledgement`, `CancellationToken`, and a new `MessageType` as construction-time, get-only members (no late binding, no `SetEndpoint`). `Message` and `Items` remain mutable, as the pipeline populates them. The constructor gains a `Type messageType` parameter and an optional `IncomingMessageContextItems` it adopts (defaulting to a fresh, lazily-allocated bag).
- [ ] `RabbitMqTopologyRuntime.ProcessDeliveryAsync` is the context factory: it inspects, resolves the endpoint via `RabbitMqTopology.TryGetEndpoint(queue, discriminator, out endpoint)`, validates the resolved type against the endpoint, then constructs the immutable-identity context (passing `inspectResult.MessageType`, `Message = inspectResult.Message`, and adopting `inspectResult.Items` as the context's bag — no copy, no per-delivery closures). The `if (inspectResult.Envelope is { } envelope)` block and every other CloudEvents reference are gone from the runtime.
- [ ] `MessageDeserializationMiddleware` invokes the endpoint's `IMessageDeserializer` with the context and `context.MessageType`; it no longer reads `CloudEventsContextKeys.Envelope`. A non-CloudEvents inspector that contributes no items flows through the stock middleware unchanged, and a deserialization failure remains a normal pipeline exception (→ NACK→DLX, covered by the tracing span).
- [ ] The inbound endpoint carries a `DeserializerType` (validated as `IMessageDeserializer`) in place of the inbound `SerializerType`; `RabbitMqInboundEndpointBuilder` exposes `WithDeserializer<TDeserializer>()` (replacing the inbound `WithSerializer`), defaulting to the default deserializer. The **outbound** target keeps `SerializerType`/`WithSerializer<T>()`/`IMessageSerializer` unchanged.
- [ ] DI registration in `AddUsf` registers the default `IMessageDeserializer` (and its concrete type, for per-endpoint resolution by `Type`) and continues to register `CloudEventMessageSerializer`/`IMessageSerializer`. Compiler startup validation rejects an inbound `DeserializerType` that is unregistered or does not implement `IMessageDeserializer`, mirroring the outbound serializer checks.
- [ ] Existing CloudEvents consume tests pass unchanged in behavior. New/updated tests cover: a non-CloudEvents inspector + custom `IMessageDeserializer` end-to-end with no CloudEvents types and no eager deserialization in the inspector; the default deserializer decoding from `TransportMessage.Body`; that the context's identity members are populated by the constructor while `Message` and `Items` stay mutable, and that a context adopts the inspector's pre-seeded `Items` instance; and a deserialization failure routing through NACK→DLX.

## Technical Details

### Split serialization from deserialization (Usf.Core)

`IMessageSerializer` loses `DeserializeAsync`, reverting to the outbound-only shape (`SerializeAsync<T>(…) → ValueTask<CloudEventEnvelope>`). Introduce:

```csharp
public interface IMessageDeserializer
{
    ValueTask<object?> DeserializeAsync(
        IncomingMessageContext context,
        Type messageType,
        CancellationToken cancellationToken = default
    );
}
```

It takes the `IncomingMessageContext` rather than a bare `TransportMessage` so a deserializer can read context items its decoding depends on — a decryption key, a schema id, a tenant-format hint, or decompressed bytes — that the inspector contributed (pre-pipeline, via the pre-seeded `Items`) or that a future pre-deserialization middleware sets. This keeps the *default* `IMessageDeserializer` usable in those scenarios instead of forcing a full replacement of the deserialization middleware. The deserializer stays CloudEvents-agnostic (it reads `context.Transport` and generic `MessageContextKey` items, never a CloudEvents type) and is contractually read-only over the context: it returns the object and must not set `context.Message` or settle `context.Acknowledgement` — the middleware owns those. `messageType` stays an explicit parameter so the *middleware* owns the target-type decision (it passes `context.MessageType`, the inspector-resolved concrete type), keeping the deserializer a plain "context + target type → object."

The default implementation is a small, CloudEvents-free class — `PayloadCodecMessageDeserializer` (name open) — holding the `IPayloadCodec` and doing exactly what `CloudEventMessageSerializer.DeserializeAsync` did, minus the envelope:

```csharp
try { return new(_payloadCodec.Decode(context.Transport.Body, messageType)); }
catch (Exception ex) { throw new MessageDeserializationException(messageType, ex); }
```

`CloudEventMessageSerializer` drops its `DeserializeAsync` member. Because USF is binary content mode only, this default deserializer is correct for both CloudEvents deliveries (data == body) and raw deliveries, so the *default* inbound path needs no CloudEvents type at all. A per-endpoint `WithDeserializer<T>()` remains the escape hatch for genuinely different deserialization (e.g. Protobuf framing that the codec abstraction does not cover); endpoints that only switch wire codecs can keep the default deserializer and swap `IPayloadCodec`.

### Extracted item bag (`IncomingMessageContextItems`, Usf.Core)

The strongly-typed item store currently inlined on `IncomingMessageContext` (the `_items` dictionary plus `SetItem`/`TryGetItem`/`GetRequiredItem`/`RemoveItem`) moves into a small reusable type so it can exist independently of a context:

```csharp
public sealed class IncomingMessageContextItems
{
    private Dictionary<object, object?>? _items; // lazily allocated on first SetItem

    public void SetItem<T>(MessageContextKey<T> key, T value);
    public bool TryGetItem<T>(MessageContextKey<T> key, out T? value);
    public T GetRequiredItem<T>(MessageContextKey<T> key);
    public bool RemoveItem<T>(MessageContextKey<T> key);
}
```

The bodies are lifted verbatim from `IncomingMessageContext`. `IncomingMessageContext` exposes the bag as `Items` (the four methods move there; whether to keep thin delegating shims on the context for ergonomics is a judgment call, not required). Because the bag is a standalone object, an inspector can build and seed one before any context exists, and the context can **adopt** that very instance — no copy, no closures. The inner dictionary stays lazily allocated, so an inspector that contributes nothing allocates nothing.

### Slim, neutral inspection result (Usf.Core)

```csharp
public sealed record InboundMessageInspectionResult(string Discriminator, Type MessageType)
{
    public object? Message { get; init; }
    public IncomingMessageContextItems? Items { get; init; }
}
```

The result is now a genuinely immutable record — no in-place fluent mutation, so its value semantics are well-defined. An inspector that needs to contribute context items builds an `IncomingMessageContextItems`, seeds it, and sets `Items`; the runtime adopts that instance as the context's bag. The entries are generic `MessageContextKey<T>` values — neutral, type-safe, and unaware of CloudEvents. Per-delivery cost is one bag (plus its lazy dictionary) for the CloudEvents case and **zero allocations for a raw inspector that contributes nothing**, with no copy from result to context. `Message` stays for the rare inspector that already holds a deserialized object (the eager-deserialize escape hatch); the runtime assigns it to `context.Message` and the deserialization middleware's `if (Message is null)` skip leaves it intact.

`IInboundMessageInspector.InspectAsync(TransportMessage, CancellationToken)` is unchanged in signature — it deliberately does **not** receive the live context, which is what keeps the context's identity immutable (below) and keeps the inspector a pure router with no endpoint-dispatch responsibility. `CloudEventsInboundMessageInspector` is unchanged except that it stops constructing the result via the `CloudEventEnvelope` overload and instead pre-seeds an `IncomingMessageContextItems` with the envelope.

### Immutable-identity context, runtime as factory

`IncomingMessageContext` gains `public Type MessageType { get; }` set from the constructor, which takes a `Type messageType` parameter plus an optional `IncomingMessageContextItems` it adopts (defaulting to a fresh, lazily-allocated bag), alongside the existing `transport`/`endpoint`/`services`/`acknowledgement`/`cancellationToken`. The identity members stay get-only. There is **no** `SetEndpoint` and no nullable `Endpoint`: the context is still built after dispatch, exactly as today, so `Endpoint` is immutable by construction. `Message` remains `{ get; set; }` (the deserialization middleware's job is to populate it) and `Items` remains mutable (middleware append to it). `Discriminator` is **not** added — `context.Endpoint.Discriminator` already carries it. `MessageType` is added because, pre-deserialization, the inspector-resolved concrete type (which may be more derived than `Endpoint.MessageType`, per the `IsAssignableFrom` dispatch check) is otherwise unavailable.

`RabbitMqTopologyRuntime.ProcessDeliveryAsync` becomes the single factory site:

```csharp
var inspectResult = await inspector.InspectAsync(transportMessage, cancellationToken).ConfigureAwait(false);
// _topology.TryGetEndpoint(queue, inspectResult.Discriminator, out endpoint) + the existing
// MessageType vs endpoint.MessageType validation … unchanged
var context = new IncomingMessageContext(
    transportMessage, endpoint, scope.ServiceProvider, acknowledgement, cancellationToken,
    inspectResult.MessageType, inspectResult.Items)
{
    Message = inspectResult.Message
};
await _topology.Pipeline(context).ConfigureAwait(false);
```

The `if (inspectResult.Envelope is { } envelope) context.SetItem(CloudEventsContextKeys.Envelope, envelope)` block (RabbitMqTopologyRuntime.cs:272-275) is deleted, removing the last CloudEvents reference from the transport runtime. Endpoint resolution (`RabbitMqTopology.TryGetEndpoint`, which carries `[NotNullWhen(true)]` so no separate null check is needed) and the type-mismatch validation are unchanged — the inspector still does not resolve endpoints.

### Deserialization middleware (Usf.Core)

`MessageDeserializationMiddleware` resolves `context.Endpoint.DeserializerType` as `IMessageDeserializer` and calls `DeserializeAsync(context, context.MessageType, context.CancellationToken)`; the `GetRequiredItem(CloudEventsContextKeys.Envelope)` call is removed. The `if (context.Message is null)` guard stays so an inspector-provided `Message` (or a prior middleware) short-circuits. The middleware remains the replaceable per-topology first step (`UseDeserializationMiddleware<T>`); it is now neutral, so the only CloudEvents-specific inbound component left is `CloudEventsInboundMessageInspector` plus the optional `CloudEventsContextKeys.Envelope` item it contributes for handlers wanting parsed metadata.

### Inbound endpoint / definition / builder / compiler (Usf.Transport.RabbitMq + Core)

Rename the inbound serializer concept to deserializer, leaving outbound untouched:

- `InboundEndpoint` (Core): `SerializerType` → `DeserializerType`; the constructor guard becomes `typeof(IMessageDeserializer).IsAssignableFrom(DeserializerType)`.
- `RabbitMqInboundEndpoint` and `RabbitMqInboundHandlerDefinition`: `SerializerType` → `DeserializerType`.
- `RabbitMqInboundEndpointBuilder`: `_serializerType` default becomes the default deserializer type; `WithSerializer<TSerializer> where TSerializer : IMessageSerializer` becomes `WithDeserializer<TDeserializer> where TDeserializer : class, IMessageDeserializer`.
- `RabbitMqTopologyCompiler`: the inbound validation at RabbitMqTopologyCompiler.cs:1167 checks the `DeserializerType` is registered **and** implements `IMessageDeserializer`; `CreateEndpointCore` (≈line 514) threads `DeserializerType`. The outbound `_resolveSerializer`/`IMessageSerializer` validation path (the target side, ≈lines 284, 874-887) is unchanged.

`RabbitMqOutboundTarget*` and `RabbitMqOutboundTargetBuilder.WithSerializer<T>` keep `IMessageSerializer` and `SerializerType` verbatim.

### DI registration (Usf.Core)

In `AddUsf`, keep `CloudEventMessageSerializer` + `IMessageSerializer` as-is, and add the default deserializer so it is resolvable both by interface and by its concrete type (the middleware resolves the endpoint's `DeserializerType`, a concrete `Type`, like serializer resolution does):

```csharp
services.TryAddSingleton<PayloadCodecMessageDeserializer>();
services.TryAddSingleton<IMessageDeserializer>(
    static sp => sp.GetRequiredService<PayloadCodecMessageDeserializer>());
```

`MessageDeserializationMiddleware` stays registered as today. The inbound endpoint default `DeserializerType` is `typeof(PayloadCodecMessageDeserializer)`.

### Tests

- `tests/Usf.Core.Tests/Messaging/TestSupport/ThrowingSerializer.cs` (and any other test double implementing `IMessageSerializer.DeserializeAsync`) splits: an outbound `ThrowingSerializer : IMessageSerializer` and an inbound `ThrowingDeserializer : IMessageDeserializer` for the deserialization-failure path. The failure test asserts the exception flows to NACK requeue=false.
- The custom non-CloudEvents inspector test required by [0004-0](0004-0-message-consumers.md) is realized as an inspector returning `(discriminator, type)` with no `Items` contribution, paired with a custom `IMessageDeserializer`, asserting end-to-end delivery with no CloudEvents type referenced and no eager deserialization in the inspector.
- Add a default-deserializer test decoding from `TransportMessage.Body`, and a context test asserting `Endpoint`/`Transport`/`Services`/`Acknowledgement`/`MessageType` are set at construction and have no setters while `Message`/items remain mutable.
- Review `RabbitMqTopologyRuntime`/consume tests in `tests/Transports/Usf.Transport.RabbitMq.Tests/Unit/AddRabbitMqConsumeTopologyTests.cs` for `WithSerializer` → `WithDeserializer` renames and any assertion on the removed `inspectResult.Envelope` handoff.

### Considered and deferred

- **Inspection as a real pipeline middleware.** Folding routing into the pipeline (ASP.NET-style routing middleware that calls `SetEndpoint`) would be the maximal-parity design, but inspection selects the scope, endpoint, message type, and content interpretation that the pipeline runs *over*, and is per-queue replaceable versus the per-topology pipeline. Keeping it a pre-pipeline step that hands the runtime its routing result — with the runtime building an immutable context — gets the decoupling without that larger rearchitecture.
- **User middleware before deserialization.** The pipeline is fixed at `framework-ack → deserialization → user middleware → terminal`, so `ConfigureInboundPipeline` adds middleware *after* deserialization. The supported pre-deserialization extension point today is the **inspector** (it pre-seeds `Items` with whatever the deserializer needs). Letting users insert middleware *ahead* of the deserialization step (e.g. a traced, NACK-covered decrypt/decompress that writes its output to a context item) is a pipeline-ordering feature scoped separately. The context-typed `IMessageDeserializer` signature chosen here is forward-compatible with it: no signature change will be needed when that ordering knob arrives.
- **Decoupling publishing from CloudEvents.** This slice intentionally only splits deserialize out of `IMessageSerializer`. Making `IMessageSerializer` no longer obligated to produce a `CloudEventEnvelope` (so outbound formats are pluggable, symmetric with the now-neutral inbound deserializer) is a separate, later plan; the split here gives it a clean seam to build on.
