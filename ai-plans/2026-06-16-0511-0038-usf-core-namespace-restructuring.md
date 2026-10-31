# Namespace Restructuring

## Rationale

`Usf.Core` currently places most messaging types directly in `Usf.Core.Messaging`, with technical subnamespaces for errors and serialization. Restructure the public API around the way users work with the library: common messaging concepts in the root namespace, publishing in an outbound namespace, and consuming in an inbound namespace. This keeps topology and contract concepts easy to find while removing implementation-detail buckets such as `Errors` and `Serialization`.

## Acceptance Criteria

- [x] Common topology, contract, CloudEvents envelope, payload codec, and DI APIs remain in `Usf.Core.Messaging`.
- [x] Publishing APIs, outbound target abstractions, outbound diagnostics, outbound serialization, and outbound-specific exceptions move to `Usf.Core.Messaging.Outbound`.
- [x] Consuming APIs, transport message/context/pipeline abstractions, inbound inspection/deserialization, acknowledgement, runtime, and inbound-specific exceptions move to `Usf.Core.Messaging.Inbound`.
- [x] `Usf.Core.Messaging.Errors` and `Usf.Core.Messaging.Serialization` are removed; their types are assigned to the root, outbound, or inbound namespace according to responsibility.
- [x] RabbitMQ transport, tests, and benchmarks compile against the new namespaces without compatibility shims for the old namespaces.
- [x] XML documentation references and public API examples are updated to avoid stale namespace names. Run a Release Build to verify as `<TreatWarningsAsErrors>` is enabled.
- [x] Automated tests need to be updated and run.

## Technical Details

Keep this as a mechanical breaking API change: move namespaces, folders, usings, XML documentation references, tests, and benchmarks without changing runtime behavior. Do not add obsolete forwarding types for the old namespaces; the library is not yet stable.

Use `Usf.Core.Messaging` for concepts that are shared by inbound and outbound or describe the messaging application as a whole. This includes `Topology`, `TopologyData`, topology registries/catalogs/provisioning, `UsfBuilder`, service collection extensions, message contract registry types, `CloudEventEnvelope`, payload codec types (`IPayloadCodec`, `Utf8JsonPayloadCodec`, `EncodedPayload`), and shared contract/topology exceptions such as `MessageContractNotRegisteredException`, `MessageContractRegistryValidationException`, and `TopologyValidationException`.

Use `Usf.Core.Messaging.Outbound` for the publish path. This includes `IMessagePublisher`, `MessagePublisher`, `TopologyPublisher`, `OutboundTarget`, `OutboundTarget<T>`, `IOutboundRoutableTarget<T>`, `SerializedMessage`, `IMessageSerializer`, `CloudEventMessageSerializer`, `CloudEventMetadata`, `CloudEventsOptions`, outbound diagnostics, outbound target validation, trace-context injection for outgoing headers, and outbound exceptions such as delivery, serialization, missing-target, target-mismatch, and not-routable failures.

Use `Usf.Core.Messaging.Inbound` for the consume path. This includes `TransportMessage`, `InboundEndpoint`, `IMessageHandler<T>`, `IncomingMessageContext`, context items and keys, message acknowledgement, `MessageAckMode`, middleware and pipeline types, handler invocation, `IInboundMessageInspector`, `CloudEventsInboundMessageInspector`, `IMessageDeserializer`, `PayloadCodecMessageDeserializer`, inbound runtime types, inbound endpoint selection keys, and inbound exceptions such as unknown-message, deserialization, and missing-endpoint failures.

After the namespace move, update `Usf.Transport.RabbitMq` to import the root, outbound, and inbound namespaces explicitly where needed. The RabbitMQ builder already exposes direction-specific outbound and inbound surfaces, so its public signatures should continue to align with that split.

Update tests and benchmarks by responsibility rather than by old folder names. Existing `Messaging.Errors` and `Messaging.Serialization` test folders may be renamed or flattened as appropriate, but the important check is that tests exercise the same behavior through the new public namespaces.
