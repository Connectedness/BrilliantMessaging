# 0001 plan deviations

This document records where `0001-1-rabbitmq-topology-extension.md` was later superseded by the following 0001 slices and how the current codebase aligns with those newer decisions.

In the current numbering, the "second plan" is `ai-plans/0001-1-rabbitmq-topology-extension.md`.

## What changed after `0001-1`

`0001-1` introduced the richer RabbitMQ route model: explicit fanout, direct, topic, and headers publishing; exchange-to-exchange bindings; deterministic validation; and support for multiple routes per message type.

The later plans kept that direction, but they changed the surrounding architecture substantially:

| Later plan | What it changed relative to `0001-1` | Codebase evidence |
| --- | --- | --- |
| `0001-2-move-serializer-to-core.md` | Serialization stopped being a RabbitMQ-local concern. The later work moved core serialization responsibilities out of the transport and set up the larger serializer changes that followed. | `src/Usf.Core/Messaging/IMessageSerializer.cs`, `src/Usf.Core/Messaging/Serialization/CloudEventMessageSerializer.cs` |
| `0001-3-rabbitmq-channel-pooling.md` | Publishing stopped being "open a channel per publish" and became channel-pool based. That shifted part of the RabbitMQ runtime model away from the target classes and into transport infrastructure. | `src/Transports/Usf.Transport.RabbitMq/DefaultRabbitMqChannelPool.cs`, `src/Transports/Usf.Transport.RabbitMq/RabbitMqChannelGroup.cs` |
| `0001-4-outbound-topology-restructuring.md` | This is the largest explicit supersession. Core publishing types were renamed to outbound-topology terminology, routes became address-based instead of exchange-based, and channel groups became first-class topology concepts. Binding and declaration modes also changed to `Skip`/`Active` and `Skip`/`Passive`/`Active`. | `src/Usf.Core/Messaging/OutboundTarget.cs`, `src/Usf.Core/Messaging/OutboundTopology.cs`, `src/Transports/Usf.Transport.RabbitMq/Configuration/RabbitMqAddressDefinition.cs`, `src/Transports/Usf.Transport.RabbitMq/Configuration/RabbitMqChannelGroupDefinition.cs`, `src/Transports/Usf.Transport.RabbitMq/Configuration/RabbitMqBindingMode.cs`, `src/Transports/Usf.Transport.RabbitMq/Configuration/RabbitMqDeclareMode.cs` |
| `0001-5-publisher-confirms-and-mandatory-routing.md` | `0001-1` did not model confirms, confirm timeouts, or delivery-failure exceptions. The later design moved delivery guarantees to the channel-group level and made mandatory routing depend on confirms. | `src/Usf.Core/Messaging/Errors/MessageDeliveryException.cs`, `src/Usf.Core/Messaging/Errors/MessageDeliveryFailureReason.cs`, `src/Transports/Usf.Transport.RabbitMq/Configuration/RabbitMqPublisherConfirmMode.cs`, `src/Transports/Usf.Transport.RabbitMq/RabbitMqOutboundTarget.cs` |
| `0001-6-rabbitmq-automatic-reconnect.md` | Recovery ownership was clarified later. The transport now requires RabbitMQ.Client automatic recovery, keeps one cached autorecovering connection wrapper, and teaches the pool to cooperate with recovered channels. | `src/Transports/Usf.Transport.RabbitMq/RabbitMqConnectionProvider.cs`, `src/Transports/Usf.Transport.RabbitMq/DefaultRabbitMqChannelPool.cs`, `src/Transports/Usf.Transport.RabbitMq/RabbitMqTransportModule.cs`, `src/Transports/Usf.Transport.RabbitMq/README.md` |
| `0001-7-cloudevents-serialization.md` | This replaced the old typed-serialization model. Typed publishing is now CloudEvents-first, while `SerializedMessage` remains the raw escape hatch. | `src/Usf.Abstractions`, `src/Usf.Core/Messaging/IMessagePublisher.cs`, `src/Usf.Core/Messaging/IMessageSerializer.cs`, `src/Usf.Core/Messaging/SerializedMessage.cs`, `src/Usf.Core/Messaging/Serialization/CloudEventMessageSerializer.cs` |
| `0001-8-publish-trace-and-span-ids.md` | Header handling gained transport-level trace propagation. For typed publishes, trace headers are injected after route headers and CloudEvents headers, so propagated values win on collision. | `src/Usf.Core/Messaging/TraceContextHeaders.cs`, `src/Transports/Usf.Transport.RabbitMq/RabbitMqOutboundTarget.cs`, `tests/Usf.Core.Tests/Messaging/TraceContextHeadersTests.cs` |
| `0001-9-multi-bus-support.md` | `0001-1` still assumed one outbound topology with multiple targets. The later design introduced named topologies, topology-scoped routing, keyed registrations, and per-topology message-contract dialects. | `src/Usf.Core/Messaging/TopologyName.cs`, `src/Usf.Core/Messaging/IOutboundTopologyRegistry.cs`, `src/Usf.Core/Messaging/OutboundTopologyRegistry.cs`, `src/Usf.Core/Messaging/EffectiveMessageContractRegistry.cs`, `src/Usf.Core/Messaging/MessagePublisher.cs`, `src/Usf.Core/Messaging/UsfBuilder.cs`, `src/Transports/Usf.Transport.RabbitMq/RabbitMqTransportModule.cs` |

## What the current code still keeps from `0001-1`

The later plans did not discard the RabbitMQ route-scenario work itself.

The current compiler still builds distinct RabbitMQ target shapes for the four publish scenarios:

- `RabbitMqFanoutOutboundTarget<TMessage>`
- `RabbitMqDirectOutboundTarget<TMessage>`
- `RabbitMqTopicOutboundTarget<TMessage>`
- `RabbitMqHeadersOutboundTarget<TMessage>`

These are defined in `src/Transports/Usf.Transport.RabbitMq/RabbitMqOutboundTarget.cs` and are created from the specialized definitions in `src/Transports/Usf.Transport.RabbitMq/RabbitMqOutboundTargetDefinition.cs`.

The code also still carries forward the richer topology model introduced in `0001-1`:

- exchanges, queues, queue bindings, and exchange bindings are first-class definitions
- validation still rejects invalid exchange/route combinations deterministically
- one message type can still have one default route plus additional named routes

## Where `0001-1` is no longer authoritative

`0001-1` should no longer be treated as the final source of truth for:

- core abstraction names and topology terminology
- the address model versus direct exchange references
- channel ownership and pooling
- publisher confirms and mandatory-routing behavior
- connection recovery expectations
- typed serialization shape and CloudEvents handling
- trace-context propagation
- topology registration and multi-topology publishing

Those areas are governed by `0001-4` through `0001-9` and by the code that implements them.

## Current assessment

I did not find a major feature that clearly failed to follow a later plan while still following `0001-1` instead. The main deviation is therefore not an implementation gap but a documentation gap: `0001-1` remains useful as the origin of the richer RabbitMQ route model, but it is stale wherever a later 0001 slice intentionally replaced its surrounding architecture.
