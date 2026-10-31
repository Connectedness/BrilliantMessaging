# Consuming Non-CloudEvents Messages

## Rationale

Today the only way to consume an event that is not a BMF CloudEvent (for example an AWS S3 "object created" notification delivered after a presigned-URL upload, or an SNS envelope) is to implement `IInboundMessageInspector` from scratch, register it in DI, and wire it per queue with `UseInspector<T>()`. That is a high bar for a common scenario, and because a queue has exactly one inspector, mixed formats on the same queue collapse into a single class full of `if/else` branching.

The goal is to make non-CloudEvents consumption a first-class, low-ceremony, composable story: let several small inspectors cooperate on one queue (chain of responsibility, first match wins), and offer hand-written recognizer building blocks (`WhenHeader`, `WhenContentType`, an open `When` predicate) that map a recognized delivery to a known message type without authoring a class. The design stays reflection-free so it remains Native-AOT/trim friendly; no source generator is introduced. JSON-body probing is intentionally out of scope for this change.

## Acceptance Criteria

- [x] `IInboundMessageInspector.InspectAsync` returns a nullable `InboundMessageInspectionResult?`, where `null` means "this inspector does not recognize the delivery" so inspectors can be chained.
- [x] `CloudEventsInboundMessageInspector` returns `null` (instead of throwing) when the delivery carries no CloudEvents `type` header, so it can sit in a chain; it still throws when CloudEvents headers are present but the `type` is unregistered.
- [x] When the configured inspector chain returns `null` overall (no inspector recognizes the delivery), the RabbitMQ runtime throws `UnknownInboundMessageException` (today's behavior is preserved).
- [x] `CompositeInboundMessageInspector` evaluates an ordered set of inner inspectors and returns the first non-`null` result, or `null` when none recognize the delivery.
- [x] A transport-agnostic chain builder in `Bmf.Core` composes a per-consumer inspector chain, supporting: `CloudEvents()`, `Use<TInspector>()`, `When(Func<TransportMessage,bool>)`, `WhenHeader(name)`, `WhenHeader(name, value)`, and `WhenContentType(value)`, each terminated by `As<T>()` or `As<T>(explicitDiscriminator)`.
- [x] `As<T>()` resolves the discriminator from `IMessageContractRegistry`; the `As<T>(string)` overload bypasses the registry for types that are not registered as contracts.
- [x] `CloudEvents()` and `Use<TInspector>()` register their inspector as a singleton by default, with an optional `ServiceLifetime` argument to override; recognizer inspectors are always singletons, built once at topology-compile time and reused across deliveries.
- [x] An inspector chain that produces no entries fails topology compilation.
- [x] `RabbitMqInboundConsumerBuilder` exposes `UseInspectors(Action<...> configure)` alongside the existing `UseInspector<T>()`; the configured chain is applied to the queue's consumer.
- [x] Topology compilation validates that every discriminator a recognizer maps to corresponds to a handler registered on the same queue and that the endpoint's message type is assignable from the recognizer's `T`, failing fast at startup otherwise.
- [x] Recognizers resolve the message type only; the built-in `WhenHeader`/`WhenContentType` blocks never read the body (the open `When` predicate can, but it is discouraged), and per-format framing (for example SNS's double-encoded payload) is handled by a custom `IMessageDeserializer` configured on the recognized endpoint via the existing `WithDeserializer<T>()` seam.
- [x] Existing CloudEvents-only consumers keep working unchanged with the default inspector.
- [x] The README's "Customizing the inbound pipeline" section documents the composable inspector chain and recognizer building blocks, including the recognize-here/deserialize-there pairing.
- [x] All newly public types and members carry XML doc comments (CS1591 is the Release gate).
- [x] Automated tests need to be written.

## Technical Details

### Core seam change (`Bmf.Core.Messaging.Inbound`)

Change `IInboundMessageInspector.InspectAsync` to return `ValueTask<InboundMessageInspectionResult?>`. `null` is the new "not recognized" signal. This is a breaking change to the public interface (acceptable pre-release) and ripples to `CloudEventsInboundMessageInspector` and the RabbitMQ runtime call site.

Update `CloudEventsInboundMessageInspector` so the absence of the `cloudEvents:type` header yields `null` rather than an exception; keep throwing `UnknownInboundMessageException` when the header is present but resolves to no contract (a CloudEvent we genuinely cannot handle is still an error, not a pass-through).

### Composite and recognizers

Add `CompositeInboundMessageInspector` implementing `IInboundMessageInspector` over an ordered `IReadOnlyList<IInboundMessageInspector>`; it returns the first non-`null` inner result. Add a recognizer inspector (e.g. `PredicateInboundMessageInspector`) that holds a `Func<TransportMessage,bool>` predicate plus a pre-resolved `(discriminator, messageType)`; on a match it returns an `InboundMessageInspectionResult`, otherwise `null`. `WhenHeader`/`WhenContentType` are thin predicate factories over the existing `TransportMessage.TryGetHeaderString` and `TransportMessage.ContentType` — no reflection, fully AOT-safe.

Add a transport-agnostic `InboundMessageInspectorChainBuilder` that accumulates ordered entries. Each entry is either a class inspector (the default `CloudEvents()` or `Use<TInspector>()`, resolved from DI by `Type`) or a recognizer (`When*(...).As<T>()`). Because `IMessageContractRegistry` is not available while the consumer builder runs, a recognizer entry captures `typeof(T)` (and an optional explicit discriminator) and defers discriminator resolution to topology-compile time; `As<T>(string)` records the explicit discriminator directly. `CloudEvents()` and `Use<TInspector>()` take an optional `ServiceLifetime` argument that defaults to `ServiceLifetime.Singleton`; the framework registers the inspector type with that lifetime (the default CloudEvents registration in `BmfServiceCollectionExtensions` is already singleton). Recognizer entries carry no services, so their `PredicateInboundMessageInspector` instances are built once at compile time and shared. A chain that ends up with zero entries is a misconfiguration and fails topology compilation.

Order is significant because the composite returns the first match: a broad recognizer placed before `CloudEvents()` (for example one keyed on a content type CloudEvents deliveries also use) will shadow genuine CloudEvents. The builder preserves declaration order and the README guidance calls this out.

### Per-consumer wiring (`Bmf.Transport.RabbitMq`)

The consumer currently carries a single inspector `Type` (`RabbitMqInboundConsumer.InspectorType`, resolved per delivery via `scope.ServiceProvider.GetRequiredService`). Generalize this to an ordered inspector chain on `RabbitMqInboundConsumerDefinition`/`RabbitMqInboundConsumer`: a list of entries that are each a DI-resolved `Type` or a baked recognizer. `RabbitMqInboundConsumerBuilder.UseInspector<T>()` becomes a single-entry chain; `UseInspectors(configure)` delegates to the Core chain builder. The default remains a single `CloudEventsInboundMessageInspector` entry, so unconfigured consumers are unchanged.

At topology-compile time (`RabbitMqTopologyCompiler`), resolve each recognizer's discriminator against `effectiveMessageContracts` (`GetDiscriminator`) when no explicit discriminator was supplied, and validate that the resulting discriminator exists in the queue's dispatch index (the `(QueueName, discriminator)` keys already built from `GetInboundDiscriminators`). This turns a misconfigured recognizer into a startup failure instead of a per-message `UnknownInboundMessageException`.

At runtime (`RabbitMqTopologyRuntime`), replace the single `GetRequiredService(consumer.InspectorType)` call with the consumer's pre-built chain. Keep the per-delivery hot path allocation-free: the ordered chain of recognizer inspectors is constructed once at topology-compile time and reused across deliveries, and only DI-resolved class inspectors are fetched from the per-delivery scope (singletons by default, so the lookup is cheap; the resolution still honours a scoped or transient lifetime when the caller chose one). The downstream flow is unchanged: a non-`null` result drives endpoint selection by `(QueueName, Discriminator)` and the existing `MessageType` assignability check; a `null` result throws `UnknownInboundMessageException` exactly as today. DI registration in `BmfServiceCollectionExtensions` keeps `CloudEventsInboundMessageInspector` registered for the default and `Use<TInspector>()` cases.

### Deserialization

Inspection and deserialization stay separate seams, and that separation is what makes mixed-format queues work. The inspector only answers "what is it" (`discriminator`, `messageType`); the discriminator selects the endpoint, and the endpoint carries its own `IMessageDeserializer` (configured per handler through `RabbitMqInboundHandlerBuilder.WithDeserializer<T>()`). Each recognized format therefore maps to a distinct discriminator → distinct endpoint → its own deserializer, so per-format framing composes without any new seam.

The rule this relies on: **recognizers resolve the type only.** The built-in `WhenHeader`/`WhenContentType` blocks match on cheap signals and never read the body; the open `When` predicate has access to the body but using it for parsing is discouraged — body decoding belongs to the deserializer. (The `InboundMessageInspectionResult.Message` pre-materialization seam stays available for advanced hand-written inspectors but the recognizer building blocks never use it.)

The default `PayloadCodecMessageDeserializer` decodes the body straight into the mapped CLR type via the configured `IPayloadCodec`. That is correct only when the third-party body deserializes directly into the target type. Framing such as SNS's double-encoded `Message` field, an S3 envelope projected onto a domain type, or a non-JSON wire format requires a custom `IMessageDeserializer` on that endpoint. The recognizer picks the type; the handler configures the matching deserializer:

```csharp
consumer
    .UseInspectors(chain => chain
        .CloudEvents()
        .WhenHeader("x-amz-sns-message-type").As<UploadCompleted>())
    .Handle<UploadCompleted, UploadCompletedHandler>(cfg => cfg.WithDeserializer<SnsEnvelopeDeserializer>());
```

Considered and rejected: folding deserializer selection into the recognizer chain (e.g. `As<T>().Using<SnsEnvelopeDeserializer>()`). It couples inspection to deserialization and duplicates configuration the handler builder already owns (deserializer and ack mode live together per endpoint). Keeping the two config sites separate matches the existing seam boundaries; the example above bridges them for discoverability.

### Documentation

Extend the README's existing "Customizing the inbound pipeline" section (which already introduces the Inspector/Deserializer/Middleware stages and `UseInspector<T>()`). Add the composable chain: introduce `UseInspectors(...)`, the `CloudEvents()`/`Use<TInspector>()`/`WhenHeader`/`WhenContentType`/`When` building blocks with `As<T>()`, the first-match-wins semantics, and a mixed-format example that pairs a recognizer with a custom `WithDeserializer<T>()` so the recognize-here/deserialize-there split is clear. Keep `UseInspector<T>()` documented as the single-inspector shorthand.

### Scope

Recognizers match on headers and content type only; JSON-body inspection is deliberately excluded and can follow later. No source generator is introduced — the recognizer chain is plain delegate-based code and therefore Native-AOT/trim safe; the only pre-existing reflection (`RabbitMqTopologyCompiler`'s startup `MakeGenericMethod`) is untouched.
