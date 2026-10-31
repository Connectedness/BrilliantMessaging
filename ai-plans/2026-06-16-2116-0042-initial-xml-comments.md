# 0042 - Initial XML Documentation

## Rationale

BMF is approaching its first public release. `src/Directory.Build.props` enables
`GenerateDocumentationFile` but silences the missing-doc warning with
`<NoWarn>$(NoWarn);CS1591</NoWarn>`. Because the library deliberately prefers `public` over
`internal` ("types hidden in plain sight"), the entire surface — **and the `protected` members of
the abstract base classes that users subclass** — is part of the documented API. This plan covers a
comprehensive XML-doc sweep so the suppression can be removed.

The goal is not blanket boilerplate. Documentation effort is tiered: mechanical members get a
templated one-liner, while entry points and members with non-obvious semantics, invariants, or
failure modes get explanatory prose that captures the *why* and the *how*. The existing docs
(`ICloudEvent`, `BaseCloudEvent`, `BmfUuid`, `OutboundTarget.PublishSerializedAsync`,
`RabbitMqOutboundTargetBuilder.Mandatory`) already establish this voice and are the reference
standard.

## Scope

Roughly 134 public types across the three `src/` projects. 84 files have no `<summary>` at all;
many already-documented files still have undocumented public/protected *members* (`Topology` is the
clearest case — class summary present, ~15 public methods bare). The sweep is therefore scoped by
*member*, not by file.

`protected` and `protected internal` members on the non-sealed (abstract) base classes are
externally visible to subclassers and **must** be documented. `private protected` members
(e.g. `OutboundTarget.StartPublishDiagnostics` and the `PublishDiagnostics` struct) are not
externally visible, do not trigger CS1591, and are out of scope for the visibility requirement
(they are already richly documented and should be left as-is).

## Acceptance criteria

- [x] Every externally visible member (`public`, `protected`, `protected internal`) in
      `Bmf.Abstractions`, `Bmf.Core`, and `Bmf.Transport.RabbitMq` has an XML `<summary>`, with
      `<param>`/`<typeparam>`/`<returns>` on members that take parameters or return values.
- [x] Tier-1 members (entry points, fluent builders, extension-point base classes, concept types
      with invariants) carry explanatory `<remarks>` and, where a throw is a documented part of the
      contract, `<exception>` tags. See the technical details for the list.
- [x] The `protected` extension-point surface of the abstract base classes is documented from the
      *subclasser's* perspective — what an overrider must implement, what the base guarantees, and
      what the template method calls in which order.
- [x] Cross-references use `<see cref=...>` / `<paramref=...>` consistently with the existing docs.
- [x] `<NoWarn>$(NoWarn);CS1591</NoWarn>` is removed from `src/Directory.Build.props`.
- [x] `dotnet build BMF.slnx --configuration Release` succeeds with no warnings
      (Release treats warnings as errors, so CS1591 becomes the acceptance gate).
- [x] No new automated tests are required (documentation-only change); the existing suite plus the
      Release build are the regression guard.

## Technical details

### Tiering

Classify each member into one of three tiers. Tier dictates depth, not whether it is documented.

**Tier 1 — explanatory (`<summary>` + `<remarks>`, plus `<exception>`/`<example>` where useful).**
User-facing entry points and members whose correct use depends on understanding an invariant or
failure mode. A bare summary here under-serves the user.

- Composition: `BmfServiceCollectionExtensions.AddBmf` (what is registered, `TryAdd`/`GetOrAdd…`
  idempotency, `ValidateOnStart` on `CloudEventsOptions.Source`, the two hosted services),
  `BmfBuilder` and its `MapMessageContracts` / `UseCloudEvents`.
- Fluent builders (the primary surface users type against): the RabbitMq topology/exchange/queue/
  binding builders, `RabbitMqOutboundTargetBuilder`, `RabbitMqInboundConsumerBuilder`,
  `RabbitMqInboundHandlerBuilder`, and the Core `MessageContractRegistryBuilder` /
  `MessageContractMapBuilder` / `MessagePipelineBuilder`. Every fluent method gets intent +
  constraints; mirror the existing `Mandatory` doc.
- Concept types with invariants: `ICloudEvent` / `BaseCloudEvent` (already done), `Topology`
  methods (`GetRequired*` vs `TryGet*`, with `<exception>` for the not-found/mismatch/not-routable
  throws), `OutboundTarget` / `IOutboundRoutableTarget`, `CloudEventsOptions` / `CloudEventMetadata`
  per-call override semantics, publisher confirms (`RabbitMqPublisherConfirmMode`,
  `RabbitMqPublisherConfirmDefaults`, mandatory-routing interaction), the acknowledgement model
  (`MessageAckMode`, `IMessageAcknowledgement`), the inbound pipeline (`IMessageMiddleware`,
  `MessageDelegate`, `IMessageHandler`, `IncomingMessageContext`), the codec/serializer extension
  points (`IPayloadCodec`, `IMessageSerializer`, `IMessageDeserializer`), and provisioning
  lifecycle (`ITopologyProvisioner`, `ITopologyRuntime`, the hosted services).

**Tier 1 (protected extension points) — document from the subclasser's perspective.**
These abstract base classes form the inheritance-based extension model; their `protected` members
are the contract a custom transport/target author implements:

- `Bmf.Core`: `OutboundTarget` and `OutboundTarget<T>` (the publish template methods
  `PublishSerializedCoreAsync` / `PublishTypedCloudEventAsync` an overrider must supply, the
  `PublishCoreAsync` template they plug into, the `Serializer` / `MessageContractRegistry`
  protected properties, and the `GetRaw*` / `GetRoute*` virtual hooks), `Topology`,
  `InboundEndpoint`, `TransportMessage`.
- `Bmf.Transport.RabbitMq`: `RabbitMqOutboundTarget<T>`,
  `RabbitMqRoutingKeyOutboundTarget<T>`, `RabbitMqHeadersOutboundTarget<T>`,
  `RabbitMqInboundEndpoint`.

For each, document what the base guarantees (e.g. that the base owns publish diagnostics and the
override only does transport dispatch), the call order of template/hook methods, and the meaning of
each `protected` constructor parameter.

**Tier 2 — substantive summary (one well-chosen sentence + param/return docs, no `<remarks>`).**
Public types whose *role* is not obvious from the name but which hide no trap: registries and
catalogs (`IMessageContractRegistry`, `MessageContractRegistry`, `EffectiveMessageContractRegistry`,
`TopologyRegistry` / `ITopologyRegistry`, `SingleTopologyRegistry`, `TopologyRegistrationCatalog`,
`MessageContractRegistration`), inbound plumbing (`InboundEndpoint`, `InboundEndpointSelectionKey`
+ comparer, `IInboundMessageInspector` / `CloudEventsInboundMessageInspector`,
`InboundMessageInspectionResult`, `MessageHandlerInvocation`, `TransportMessage`,
`IncomingMessageContextItems`), outbound plumbing (`MessagePublisher` / `IMessagePublisher`,
`TopologyPublisher`, `SerializedMessage`, `EncodedPayload`, `CloudEventMessageSerializer`), and the
RabbitMq channel pooling types (`IRabbitMqChannelPool` / `DefaultRabbitMqChannelPool`,
`RabbitMqChannelLease`, `RabbitMqChannelSource`, `RabbitMqConnectionProvider`, channel groups).

**Tier 3 — default boilerplate (templated, do not pad).**

- Constructors → `Initializes a new instance of the <see cref="X" /> class.` (adjust `class`/
  `record`/`struct`). Param docs only when not self-evident.
- Exception types (~13: `InboundEndpointNotFoundException`, `OutboundTargetNotFoundException`,
  `OutboundTargetNotRoutableException`, `OutboundTargetTypeMismatchException`,
  `MessageDeserializationException`, `MessageContractRegistryValidationException`,
  `MessageContractNotRegisteredException`, `CloudEventMetadataException`, `MessageDeliveryException`,
  `MessageSerializationException`, `TopologyValidationException`, `UnknownInboundMessageException`,
  `MessageDeliveryException`) → "thrown when…" type summary, boilerplate ctors, terse summaries on
  captured properties.
- `*Definition` records (the `RabbitMq*OutboundTargetDefinition`, `*ChannelGroupDefinition`,
  `RabbitMqQueue`/`Exchange`/`Binding` definitions, `TopologyData`) → one-line record summary +
  positional-param docs; these are DTO-shaped and do not warrant prose.
- Self-evident enums (`MessageAckMode`, `RabbitMqBindingMode`, `RabbitMqDeclareMode`,
  `RabbitMqOutboundRouteScenario`, `MessageDeliveryFailureReason`) → type-level `<remarks>`
  explaining when each value is chosen; one-line member summaries.
- Constant holders (`CloudEventAttributeNames` (done), `CloudEventsContextKeys`,
  `TraceContextHeaders`, `IncomingMessageContextItems`) → type summary + terse per-const summaries.
- Public-by-policy infrastructure (`ReadOnlyMemoryByteEqualityComparer`,
  `InboundEndpointSelectionKeyComparer`, `OutboundDiagnostics`, `CloudEventsOptionsValidation`,
  `OutboundTargetContractValidator`, hosted services) → honest one-liners.

### Style

Match the established voice: imperative present-tense summaries ("Creates…", "Maps…", "Publishes…");
reserve `<remarks>` for the *why* and the invariant the caller/overrider must respect; lean on
`<see cref>` for navigability.

### Sequencing and verification

The sweep is delivered as a **single commit** covering the whole surface, including the `NoWarn` removal.
Internally, work the tiers in order — Tier 1 first (entry points + builders +
extension-point base classes) to settle the depth and voice before the mechanical Tier 3 bulk and
the Tier 2 middle — but ship them together. To get an exact, member-level worklist (CS1591 fires per
member, so grepping for `<summary>` undercounts), temporarily promote the warning rather than
suppress it — e.g. build with `-warnaserror:CS1591` after removing the `NoWarn` entry — and let the
compiler enumerate the gaps. Removing the `NoWarn` line is the final step and the acceptance gate.
