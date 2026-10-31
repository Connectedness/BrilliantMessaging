# RabbitMQ Namespace Restructuring

## Rationale

`Usf.Transport.RabbitMq` already exposes direction-specific topology registration and builder surfaces, but most transport types still live directly in the root namespace and configuration types live in a technical `Configuration` namespace. Restructure the public API around the same user-facing responsibilities as `Usf.Core`: common RabbitMQ topology and broker resource concepts in the root namespace, publishing in an outbound namespace, and consuming in an inbound namespace.

## Acceptance Criteria

- [x] Direction-neutral RabbitMQ topology, broker resource, provisioning, connection, and channel infrastructure APIs remain in `Usf.Transport.RabbitMq`.
- [x] RabbitMQ publishing APIs, outbound targets, outbound target definitions, publisher channel groups, publisher-confirm options, and outbound route scenarios move to `Usf.Transport.RabbitMq.Outbound`.
- [x] The outbound-only `RabbitMqChannelGroup` and `RabbitMqChannelGroupDefinition` types are renamed to `RabbitMqOutboundChannelGroup` and `RabbitMqOutboundChannelGroupDefinition`.
- [x] RabbitMQ consuming APIs, inbound consumers, inbound endpoints, inbound channel groups, runtime, transport message, and acknowledgement types move to `Usf.Transport.RabbitMq.Inbound`.
- [x] `Usf.Transport.RabbitMq.Configuration` is removed; its types are assigned to the root, outbound, or inbound namespace according to responsibility.
- [x] Tests and any benchmark or sample imports compile against the new RabbitMQ namespaces without compatibility shims for the old namespaces.
- [x] XML documentation references and public API examples are updated to avoid stale namespace names. Run a Release build to verify as `<TreatWarningsAsErrors>` is enabled.
- [x] Automated tests need to be updated and run.

## Technical Details

Keep this as a mechanical breaking API change: move namespaces, folders, usings, XML documentation references, tests, and any benchmark or sample imports without changing runtime behavior. Do not add obsolete forwarding types for the old namespaces; the library is not yet stable.

Use `Usf.Transport.RabbitMq` for APIs that are shared by publishing and consuming or describe a RabbitMQ topology as a whole. This includes `RabbitMqTransportModule`, `RabbitMqTopology`, `RabbitMqTopologyBuilder`, `RabbitMqTopologyConfiguration`, `RabbitMqTopologyCompiler`, `RabbitMqTopologyProvisioner`, `IRabbitMqTopologyBuilder<TSelf>`, connection and channel infrastructure (`RabbitMqConnectionProvider`, `RabbitMqChannelSource`, `IRabbitMqChannelPool`, `DefaultRabbitMqChannelPool`, and `RabbitMqChannelLease`), and broker resource builders and definitions (`RabbitMqExchangeBuilder`, `RabbitMqQueueBuilder`, `RabbitMqQueueBindingBuilder`, `RabbitMqExchangeBindingBuilder`, exchange/queue/binding definitions, `RabbitMqDeclareMode`, and `RabbitMqBindingMode`).

Use `Usf.Transport.RabbitMq.Outbound` for the publish path. This includes `IRabbitMqOutboundTopologyBuilder`, `RabbitMqOutboundTargetBuilder<TMessage>`, `RabbitMqOutboundTarget<TMessage>` and route-specific target types, outbound target definition records, `RabbitMqOutboundRouteScenario`, `RabbitMqOutboundChannelGroup`, `RabbitMqOutboundChannelGroupDefinition`, `RabbitMqPublisherConfirmMode`, and `RabbitMqPublisherConfirmDefaults`.

Use `Usf.Transport.RabbitMq.Inbound` for the consume path. This includes `IRabbitMqInboundTopologyBuilder`, `RabbitMqInboundConsumerBuilder`, `RabbitMqInboundConsumerDefinition`, `RabbitMqInboundHandlerBuilder`, `RabbitMqInboundHandlerDefinition`, `RabbitMqInboundEndpoint`, `RabbitMqInboundEndpoint<TMessage>`, `RabbitMqInboundConsumer`, `RabbitMqInboundChannelGroup`, `RabbitMqInboundChannelGroupDefinition`, `RabbitMqTopologyRuntime`, `RabbitMqTransportMessage`, and `RabbitMqMessageAcknowledgement`.

Rename the current outbound-only `RabbitMqChannelGroup` and `RabbitMqChannelGroupDefinition` types to `RabbitMqOutboundChannelGroup` and `RabbitMqOutboundChannelGroupDefinition` while moving them to the outbound namespace. The old names imply channel groups are only an outbound concept even though inbound topologies also define consumer channel groups.

Leave `RabbitMqTopology` and `RabbitMqTopologyCompiler` in the root namespace for this plan. A single RabbitMQ topology intentionally owns one connection and may contain both outbound targets and inbound consumers. The compiler already has separate outbound and inbound sections, but splitting compiler internals should be handled separately if it becomes useful.
