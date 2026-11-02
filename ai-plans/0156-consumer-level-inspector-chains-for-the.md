# Consumer-Level Inspector Chains for the NATS Transport

> Issue: [#156](https://github.com/Connectedness/BrilliantMessaging/issues/156)

## Rationale

#55 added per-consumer inspector chains, but only the RabbitMQ transport exposes them. NATS hard-wires one CloudEvents inspector, so a delivery without CloudEvents headers is dead-lettered or terminated. This blocks third-party events on JetStream, such as bucket notifications that MinIO and RustFS publish in [S3's event message format](https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-content-structure.html). NATS should offer the same chain API and compile-time validation as RabbitMQ.

This is one story delivered as a stack: moving the chain to Core changes RabbitMQ and fixes a RabbitMQ bug, and that is easier to review apart from the new NATS behavior that depends on it.

## Acceptance Criteria

### Layer 1: Shared chain

- [ ] The compiled chain and its entries live in `BrilliantMessaging.Core.Messaging.Inbound` as `CompiledInboundMessageInspectorChain`, `CompiledInboundMessageInspectorChainEntry`, `CompiledServiceInboundMessageInspectorChainEntry`, and `CompiledInstanceInboundMessageInspectorChainEntry`. The RabbitMQ-prefixed types are removed.
- [ ] A public Core helper performs chain validation, recognizer discriminator resolution, recognizer-to-handler mapping, and chain compilation. `RabbitMqTopologyCompiler` uses it, and its validation messages are unchanged.
- [ ] A `CloudEvents()` entry in a RabbitMQ chain resolves types against the topology's effective message contract registry, covered by a regression test for a topology that publishes and consumes a topology-locally mapped type.
- [ ] The XML docs on `CloudEvents()` state that its `ServiceLifetime` and any custom DI registration of that inspector are ignored at runtime.
- [ ] All public types and members added in this layer carry XML doc comments.
- [ ] Automated tests cover the moved chain and the shared helper.

### Layer 2: NATS

- [ ] `NatsInboundConsumerBuilder` exposes `UseInspector<TInspector>(ServiceLifetime)` and `UseInspectors(Action<InboundMessageInspectorChainBuilder>)` with the same semantics as `RabbitMqInboundConsumerBuilder`.
- [ ] Consumers without an inspector configuration keep today's behavior: a single CloudEvents inspector resolved against the topology's effective message contract registry, including topology-local contracts.
- [ ] A `CloudEvents()` entry in a NATS chain also resolves types against the effective registry.
- [ ] `NatsTopologyCompiler` rejects an empty chain, service inspectors that don't implement `IInboundMessageInspector` or aren't registered, and `As<T>()` on an unregistered type (with a hint to `As<T>(explicitDiscriminator)`). Every recognizer discriminator must map to one handler on the same consumer: the handler for exactly the recognizer's `T` if there is one, otherwise the only handler whose message type is assignable from `T`. No match or several assignable matches fail validation.
- [ ] A NATS handler without a registered contract is valid when a recognizer on the same consumer maps to it. Recognizer discriminators join the consumer's dispatch table, take part in duplicate detection, and supply the primary discriminator and default endpoint name when no contract exists.
- [ ] Inspector types from `UseInspector<T>()` / `Use<T>()` are auto-registered with their configured lifetime without overwriting existing registrations.
- [ ] At runtime the chain is evaluated in declaration order (first match wins). Recognizer inspectors are built once at compile time; only DI-resolved inspectors are fetched from the per-delivery scope.
- [ ] NATS settlement semantics stay unchanged: an inspector exception, a `null` chain result, an unknown discriminator, or a non-assignable message type still dead-letters or terminates the message immediately without consuming retries.
- [ ] A delivery without CloudEvents headers can be recognized by a `When`/`WhenHeader` recognizer and handled by an endpoint with a custom `IMessageDeserializer` configured through `WithDeserializer<T>()`.
- [ ] `docs/nats-transport.md` documents inspector chains, including:
  - the recognize-here/deserialize-there pairing
  - recognizers placed before `CloudEvents()` shadowing genuine CloudEvents
  - `TransportMessage.Source` being the NATS subject
  - third-party publishers possibly not setting `content-type`
  - manual registration of custom deserializers
  - inspectors having to be deterministic and avoid I/O, because a throwing inspector counts as a poison message and `AckProgress` doesn't run during inspection
  - handler types without a registered contract requiring a recognizer, so a custom inspector alone cannot route to them
  - `ce-` headers being exposed to recognizers under the `cloudEvents:` prefix, so `WhenHeader("ce-type")` never matches
- [ ] `docs/nats-transport.md` also covers S3 specifics: one delivery carries a `Records` array and is settled as a unit, object keys are URL-encoded, and handlers and deserializers must tolerate the `s3:TestEvent` shape without `Records`.
- [ ] All public types and members added in this layer carry XML doc comments.
- [ ] Automated tests are written, including a broker integration test that consumes a JetStream message with an S3 event notification body and no CloudEvents headers end to end.

## Technical Details

### Stack Design

1. `0156-consumer-level-inspector-chains-for-the-shared-chain` — move the compiled chain and validation to Core, and fix the RabbitMQ registry gap through them.
2. `0156-consumer-level-inspector-chains-for-the-nats` — add chains to the NATS transport; depends on the shared chain layer.

### Shared helper

Add a new public static class in `BrilliantMessaging.Core.Messaging.Inbound`, `InboundMessageInspectorChainCompiler`, extracted from `ValidateInspectorChain`, `ResolveRecognizers`, `MapRecognizersToHandlers`, and `CreateInspectorChain` in `RabbitMqTopologyCompiler`. Those methods take RabbitMQ definition types and hard-code "queue" in their messages, so the helper is parameterized over the handler's message type and the consumer description. RabbitMQ keeps its exact wording, which existing tests assert.

`RabbitMqInboundMessageInspectorChainTests` moves to the Core tests, and the `CompositeInboundMessageInspector` remarks that name the RabbitMQ chain are updated.

### CloudEvents and topology-local contracts

Chain compilation turns service entries for `CloudEventsInboundMessageInspector` into instance entries bound to the topology's effective registry. This also covers `Use<CloudEventsInboundMessageInspector>()`. The DI singleton only knows the canonical registry, which is the cause of the RabbitMQ bug. `ValidateMessageContractDialect` requires an outbound target for dialect-mapped types, so that bug only affects topologies that both publish and consume such a type.

`AddRabbitMqConsumeTopologyTests` currently asserts a service entry for `CloudEvents()` and must assert the instance entry instead.

### NATS builder and definition

Add an `ImmutableArray<InboundMessageInspectorChainEntry>` to `NatsInboundConsumerBuilder` and `NatsInboundConsumerDefinition`, defaulting to a single `CloudEvents()` entry, mirroring `RabbitMqInboundConsumerBuilder`. Register service inspector types in `AddNatsTopologyCore` the same way `RabbitMqTransportModule.RegisterInspector` does.

### Handlers without a contract

`ValidateConsumers` rejects handlers without a contract, and `CompileConsumers` calls `GetDiscriminator` unconditionally and only indexes contract discriminators. Port the behavior of `CreateInboundEndpointPlans` and the matching checks in `RabbitMqTopologyCompiler.ValidateConsumers`. The default endpoint name derived during validation must keep matching compilation. Handlers with neither a contract nor a recognizer keep the current NATS error messages.

### NATS runtime

Store the compiled chain on `NatsInboundConsumer`, and replace the `_inspector` field in `NatsTopologyRuntime` with it. Create the per-delivery scope before inspection, which intentionally gives up the scope-free path for unroutable deliveries from `42cfac3`. A DI resolution failure lands in the existing inspection catch branch. Keep all settlement branches, and update the comment in `DispatchAsync` that calls inspection pure computation. Also update the CloudEvents-only wording in the "Consuming" section of `docs/nats-transport.md` and in the `NatsInboundConsumer.EndpointsByDiscriminator` XML summary.

### S3 event notifications

The S3 format describes only the body: a top-level `Records` array whose entries use camelCase fields. The docs example therefore recognizes these messages by subject through `When(message => ...)` on `TransportMessage.Source`.

`Utf8JsonPayloadCodec` defaults to `JsonSerializerOptions.Default`, which is case-sensitive and ignores unknown members. The default deserializer therefore needs a message type that maps the wire names, for example with `[JsonPropertyName]`; otherwise the fields silently stay unset. The integration test uses a custom `IMessageDeserializer`.

### Out of scope

InMemory transport parity is tracked in [#157](https://github.com/Connectedness/BrilliantMessaging/issues/157).
