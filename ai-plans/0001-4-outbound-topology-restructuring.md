## Rationale

For USF, we want to introduce two new terms: Outbound Topology and Inbound Topology. The former addresses everything regarding sending/publishing messages, the latter everything related to consuming messages.

USF should make outbound messaging topology explicit before inbound messaging is added. The core publishing model should use outbound terminology, with `OutboundTarget` as the message-type-specific executable route. RabbitMQ should expose addresses and channel groups as first-class outbound topology concepts while keeping broker details visible and configurable. A `RabbitMqOutboundTopology` should own its channel groups, compiled outbound targets, provisioning model, and disposal lifecycle, and should own exactly one outbound RabbitMQ connection through a dedicated `RabbitMqConnectionProvider`. The current `RabbitMqConnectionManager` should be reshaped into that provider: a lifecycle-only component (lazy creation, maintenance, disposal of one connection) with the channel-budget validation moved out onto the topology, so the provider stays cohesive and reusable for the future inbound topology.

## Acceptance Criteria

- [x] Core publishing types are renamed to outbound terminology, including `Target` to `OutboundTarget`, `Target<T>` to `OutboundTarget<T>`, `IMessageTopology` to `IOutboundTopology`, `MessageTopology` to `OutboundTopology`, `ITargetRegistry` to `IOutboundTargetRegistry`, `ITopologyProvisioner` to `IOutboundTopologyProvisioner`, `MessageTopologyValidationException` to `OutboundTopologyValidationException`, `MessageTargetNotFoundException` to `OutboundTargetNotFoundException`, `MessageTargetTypeMismatchException` to `OutboundTargetTypeMismatchException`, and related hosted-service and test names.
- [x] Diagnostics are renamed to outbound terminology, including the emitted activity and instrument names: the `usf.messaging.*` activity, metric, and tag names become `usf.outbound.*`. The library was never released, so these breaking telemetry changes are acceptable.
- [x] RabbitMQ publishing types are renamed to outbound terminology, including the builder, configuration, compiler, compiled topology, provisioner, target base class, and concrete target classes.
- [x] `RabbitMqConnectionManager` is reshaped into a lifecycle-only `RabbitMqConnectionProvider` that lazily creates, maintains, and disposes exactly one outbound RabbitMQ connection, preserving single-flight creation and idempotent disposal, and carrying no channel-budget or topology knowledge.
- [x] Channel-budget validation against the broker's negotiated `channel_max` is moved onto `RabbitMqOutboundTopology`, which performs it once on first connection acquisition through the provider.
- [x] `RabbitMqOutboundTopology` owns one `RabbitMqConnectionProvider`, exposes a channel seam to its channel groups and provisioner, and disposes the provider last.
- [x] `RabbitMqPublishingConfiguration.ConnectionFactoryFactory` is renamed to `CreateConnectionFactory`.
- [x] RabbitMQ addresses are introduced as first-class outbound topology definitions, so targets reference an address rather than an exchange directly.
- [x] Multiple outbound targets can publish to the same RabbitMQ address while retaining independent serializers, routing-key/header behavior, names, and message types.
- [x] `IMessagePublisher` exposes a distinctly named non-generic `PublishRawAsync` that publishes an already prepared `SerializedMessage` through an explicit `OutboundTarget` without invoking USF serialization. It is a separate method, not an overload of `PublishMessageAsync<T>`, so a `SerializedMessage` cannot bind to the generic typed path.
- [x] `PublishUntypedAsync(object, ...)` is removed; the non-generic `OutboundTarget` exposes a `PublishSerializedAsync` dispatch primitive that both the typed path and `PublishRawAsync` flow through, and typed/object mismatch handling stays in the publisher.
- [x] RabbitMQ channel groups are introduced as first-class outbound topology definitions, so two or more targets can intentionally share a channel pool.
- [x] The current per-target/shared channel-pooling mode is replaced by channel groups while preserving the default behavior of one private single-channel group per target.
- [x] Channel pools are owned by channel groups rather than targets, so targets no longer own or dispose pools; disposal flows topology → channel groups → connection provider.
- [x] RabbitMQ exchange and queue declaration remains explicit and uses `RabbitMqDeclareMode` with `Skip`, `Passive`, and `Active` values.
- [x] RabbitMQ binding configuration uses `RabbitMqBindingMode` with `Skip` and `Active` values, because RabbitMQ has no passive binding declaration.
- [x] Outbound topology compile-time validation rejects duplicate targets, duplicate addresses, duplicate channel groups, unknown address references, unknown channel group references, address references to unknown exchanges, missing serializers, invalid exchange/route combinations, and invalid channel group sizes. The compiler also computes the worst-case channel budget (sum of distinct channel-group maximums, including implicit per-target groups), which the topology later enforces against the broker's `channel_max`.
- [x] Outbound topology tests and RabbitMQ integration tests are updated for the renamed API, shared addresses, channel groups, passive and active exchange declaration, and connection disposal order.

## Technical Details

The core namespace should move from message-publishing terminology toward outbound topology terminology. `OutboundTarget` remains the non-generic abstraction used by topology dictionaries, explicit target lookup, diagnostics, explicit target passing, and raw outbound payload dispatch. `OutboundTarget<T>` remains the typed hot path and continues to own the serializer and dispatch logic for one message type. `IMessagePublisher` should remain focused on publish semantics and depend on `IOutboundTopology` and `OutboundTarget`. Sending and request-response should be introduced as separate outbound-facing abstractions in future slices, such as `IMessageSender` and a request-response client, instead of widening the publisher abstraction.

`IMessagePublisher` should expose a distinctly named non-generic `PublishRawAsync(SerializedMessage message, OutboundTarget target, CancellationToken cancellationToken = default)`. It must be a separate method rather than an overload of `PublishMessageAsync<T>`, otherwise a `SerializedMessage` argument would bind to the generic typed path with `T = SerializedMessage` and be serialized a second time. `PublishRawAsync` bypasses USF serialization and dispatches the supplied serialized payload directly through the target; the caller owns the message envelope (content type, message id, correlation id, headers). The typed `PublishMessageAsync<T>` path remains the default application-message API and should continue to use `OutboundTarget<T>` when possible to avoid boxing and keep serializer ownership on the target.

`PublishRawAsync` must require an explicit `OutboundTarget`; it must not attempt default topology resolution because an already serialized message has no CLR message type that can be used for target lookup. `OutboundTarget` should not require a CLR message type. If a type indicator is useful for diagnostics, expose it as nullable metadata or only on `OutboundTarget<T>`.

The old `PublishUntypedAsync(object message, ...)` method should be removed from the target abstraction. The non-generic `OutboundTarget` should instead expose serialized-payload dispatch, `PublishSerializedAsync(SerializedMessage message, CancellationToken cancellationToken = default)`. This is the dispatch primitive that both paths flow through: `OutboundTarget<T>.PublishAsync` serializes the typed message and then calls `PublishSerializedAsync`, and the publisher's `PublishRawAsync` calls it directly. (Note the deliberate naming split across layers: the publisher's escape hatch is `PublishRawAsync`, while the target's primitive is `PublishSerializedAsync`, because from the target's perspective it is not "raw" — the typed path uses it too.) `IMessagePublisher` should perform the bridge decision for the typed path: if the resolved or explicit target is `OutboundTarget<T>`, use the typed path; otherwise throw an `OutboundTargetTypeMismatchException` instead of passing the typed message as `object`. This keeps the struct hot path unboxed and separates typed object publishing from raw serialized payload publishing.

A concrete sketch of the target shape is:

```csharp
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Usf.Core.Messaging;

public abstract class OutboundTarget
{
    public string Name { get; }

    public string TransportName { get; }

    public virtual Type? MessageType => null;

    public abstract Task PublishSerializedAsync(
        SerializedMessage message,
        CancellationToken cancellationToken = default);
}

public abstract class OutboundTarget<T> : OutboundTarget
{
    public sealed override Type MessageType => typeof(T);

    public async Task PublishAsync(T message, CancellationToken cancellationToken = default)
    {
        // ...
    }
}
```

`OutboundTopology` should keep the current two lookup surfaces: one default target per exact message type and named targets through `IOutboundTargetRegistry`. The default publish path remains exact-type resolution. Named targets remain the escape hatch for additional explicit routes for the same message type.

The RabbitMQ configuration model should separate broker entities, addresses, targets, and channel groups. Exchanges, queues, and bindings remain RabbitMQ broker entities and keep their explicit declaration or binding settings. Outbound topology should not artificially forbid queue or binding declaration, because declaration is operational broker setup and some applications need to manage all required broker entities from one composition root. Documentation should still teach the intended split: outbound topologies should usually target exchanges through outbound addresses, while inbound topologies should usually target exchanges, queues, and bindings as part of receive endpoint setup. A new `RabbitMqAddressDefinition` should represent an outbound destination such as an exchange-backed address. In this slice, an address can be RabbitMQ-specific and contain the referenced exchange name. The address carries the destination only; routing key, headers, and exchange-scenario behavior stay on the target. This is what keeps "multiple targets, same address, independent routing" coherent, and it means the existing route-scenario-versus-exchange-type validation naturally enforces that every target sharing an address agrees with that one exchange's type. Publish target configuration should reference `AddressName` instead of `ExchangeName`; concrete target compilation resolves the address to its exchange during validation and compile.

Channel groups should replace `RabbitMqChannelPoolingMode`, `MaxChannelsPerTarget`, and `SharedChannelPoolSize` as the primary configuration model. A `RabbitMqChannelGroupDefinition` should at least contain a name and maximum channel count. Targets may reference a channel group by name. If a target does not specify a channel group, the compiler creates an implicit private single-channel group for that target, so existing per-target ordering behavior remains the default. Explicitly named channel groups allow multiple targets to share the same bounded pool. Worst-case channel count is the sum of distinct channel group maximums.

`RabbitMqConnectionManager` should be reshaped into a lifecycle-only `RabbitMqConnectionProvider` (the name avoids colliding with `RabbitMQ.Client`'s connection types). The provider owns exactly one outbound connection and does only three things: lazily create it on first request behind a single-flight gate, hand it out, and dispose it. Its constructor should take a `Func<CancellationToken, Task<IConnection>>` connection-creation delegate (one constructor; test code injects a fake delegate), so the provider depends on neither `RabbitMqPublishingConfiguration` nor channel-budget concerns and is reusable for the future inbound topology. The current type's channel-budget fields, `ValidateChannelCapacity`, the `RabbitMqChannelBudget` dependency, and the create-time dispose-on-failure wrapper (which only existed to clean up after a failed budget check) all leave the provider; what stays is the gate, the `_connectionTask` double-check, `ThrowIfDisposed`, and idempotent `Interlocked.Exchange` disposal across `Dispose`/`DisposeAsync`.

`RabbitMqOutboundTopology` should own one `RabbitMqConnectionProvider`, build its connection-creation delegate from the configured `CreateConnectionFactory`, expose a channel seam to its channel groups and provisioner (drawing channels from the provider's connection), and dispose channel groups before disposing the provider. The topology also owns channel-budget enforcement: on first connection acquisition it compares the compiler-computed worst-case channel count against `IConnection.ChannelMax` and throws `OutboundTopologyValidationException` on overflow. The provisioner draws a transient channel through the same seam rather than a pooled lease.

Provisioning should be renamed to outbound topology provisioning and operate from `RabbitMqOutboundTopology`. For this outbound-focused plan, exchange, queue, queue-binding, and exchange-binding provisioning may all remain available from the RabbitMQ outbound builder. Outbound targets should still publish only through addresses, and RabbitMQ addresses should resolve to exchanges in this slice. Queues, dead-lettering, skipped messages, prefetch, concurrency, consumers, and receive endpoint behavior remain inbound topology concerns even though broker entity declaration is available in both topology builders.

RabbitMQ entity configuration should replace `RabbitMqDeclareMode.None` with `RabbitMqDeclareMode.Skip`, replace `RabbitMqDeclareMode.Ensure` with `RabbitMqDeclareMode.Active`, and keep `RabbitMqDeclareMode.Passive` for exchange and queue passive checks. Binding configuration should replace `RabbitMqBindingDeclareMode` with `RabbitMqBindingMode`, containing only `Skip` and `Active`. Builder method names should avoid calling binding behavior passive declaration; RabbitMQ binds, and active binding is idempotent broker setup.

The compiler should validate the full configuration deterministically before building runtime objects. Validation should group and sort errors consistently, including duplicate address and channel group names, missing referenced addresses or groups, address references to missing exchanges, route/exchange-type mismatches, duplicate default targets, duplicate named targets, missing or unregistered serializers, invalid channel group sizes, and unsupported declaration or binding modes. The compiler also computes the worst-case channel budget — the sum of distinct channel-group maximums, counting each implicit per-target single-channel group — and hands that number to the topology. Channel-budget overflow cannot be validated at compile time because the broker's negotiated `channel_max` is only known after the connection is established, so the topology enforces it on first connection. Other broker-state incompatibilities that cannot be proven from configuration should continue to fail during passive or active provisioning.

Tests should follow the renamed API and cover the new model sociably through the RabbitMQ builder where possible. Unit tests should verify address sharing, channel group sharing, implicit private channel groups, channel budget calculation, and disposal order. Integration tests should publish multiple message types through the same address and verify routing outcomes, and should cover both passive and active exchange declaration behavior.
