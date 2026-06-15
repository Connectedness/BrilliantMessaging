## Rationale

`RabbitMqAddressDefinition` was introduced (see `0001-4-outbound-topology-restructuring.md`) so that outbound targets reference a named address rather than an exchange directly. In practice the address is a pure alias: a `record(Name, ExchangeName)` that carries nothing beyond an exchange name, is resolved away at compile time, and is never consulted at runtime. It provides indirection without abstraction — it does not hide RabbitMQ, adds no routing semantics, and enables no 1:N or N:1 mapping. It also makes the outbound surface gratuitously asymmetric with the inbound surface, where consumers reference queues directly with no intermediate concept.

The goal of this plan is to remove the address concept entirely and have outbound targets reference exchanges directly, mirroring how inbound consumers reference queues directly. `OutboundTarget` remains the single transport-agnostic abstraction a message is targeted at. This is a pre-1.0 breaking change, which the codebase explicitly permits.

## Acceptance Criteria

- [x] `RabbitMqAddressDefinition` and the `Address(string name, string exchangeName)` builder method (and its `IRabbitMqOutboundTopologyBuilder` interface member) are removed.
- [x] Outbound targets reference an exchange by name directly: `RabbitMqOutboundTargetDefinition.AddressName` becomes `ExchangeName`, and the `RabbitMqOutboundTargetBuilder<TMessage>` route verbs are renamed to `ToFanoutExchange`/`ToDirectExchange`/`ToTopicExchange`/`ToHeadersExchange` (the `Exchange` suffix keeps the destination kind explicit at the call site).
- [x] `RabbitMqTopologyConfiguration.Addresses` and `RabbitMqTopology.Addresses` are removed, along with the corresponding builder backing field and constructor arguments.
- [x] The compiler resolves a target's exchange in a single lookup (`exchangesByName[target.ExchangeName]`) instead of the two-hop address→exchange lookup.
- [x] Address-specific validation is removed: the duplicate-address pass and `ValidateAddressDefinitions` are deleted, and `ValidateTarget` checks the referenced exchange directly. The scenario↔exchange-type validation (`ValidateTargetAgainstExchange`) is preserved unchanged.
- [x] User-facing validation and error wording that referenced "address" (e.g. the "references unknown address" error and `GetTargetDescription`) is updated to refer to the exchange.
- [x] All XML-doc `<see cref>` and prose references to the removed `Address` member and address concept are updated, so Release builds emit no CS1574 (dangling cref) or other warnings under `<TreatWarningsAsErrors>`.
- [x] Explicit usings are retained; no global or implicit usings are introduced.
- [x] Existing unit and integration tests are updated for the renamed API, and any address-specific tests are removed or repurposed to exchange-reference tests.
- [x] Plan documents under `ai-plans/` that describe the now-removed address concept are left as historical record; only the live code and tests change.

## Technical Details

This is a mechanical collapse of a `target → address → exchange` indirection into `target → exchange`. The change is contained within `Usf.Transport.RabbitMq`; `Usf.Core` is unaffected because `OutboundTarget`/`OutboundTarget<T>` never knew about addresses, and `RabbitMqOutboundTarget` already binds directly to an exchange name (`_exchangeName`).

**Definitions and builders.** Delete `Configuration/RabbitMqAddressDefinition.cs`. In `RabbitMqOutboundTargetDefinition` (and all derived records), rename the `AddressName` positional parameter to `ExchangeName`. In `RabbitMqOutboundTargetBuilder<TMessage>`, rename the `_addressName` field and rename the `ToXxxAddress(...)` methods to `ToFanoutExchange`/`ToDirectExchange`/`ToTopicExchange`/`ToHeadersExchange`; keep their signatures otherwise identical (the routing-key/header overloads are unchanged). The "must select an address" guard in `Build` becomes "must select an exchange". In `RabbitMqTopologyBuilder`, remove `_addressDefinitions`, the `Address(...)` method, and its explicit-interface bridge; remove the addresses argument from the `Build()` call. In `IRabbitMqOutboundTopologyBuilder`, remove the `Address(...)` member.

**Doc comments.** Several XML-doc summaries reference the address concept and will produce dangling-`cref` warnings (CS1574, fatal under `<TreatWarningsAsErrors>`) once `Address` is deleted — notably the `RabbitMqTopologyBuilder` class summary, which enumerates `<see cref="Address" />`, and the `IRabbitMqOutboundTopologyBuilder` summary, which advertises addresses. Grep for `cref="Address` and for "address" in doc comments across the project and update every hit as part of the change, not as a follow-up.

**Configuration and compiled topology.** Remove the `Addresses` parameter from `RabbitMqTopologyConfiguration` and the `Addresses` property + constructor argument from `RabbitMqTopology` (it is stored but never read, so nothing downstream breaks — `RabbitMqTopologyProvisioner` only declares exchanges, queues, and bindings).

**Compiler.** In `CompileOutbound`, drop the `addressesByName` dictionary; replace the two-hop resolution with `exchangesByName[targetDefinition.ExchangeName].Name`. In `Validate`/`ValidateTarget`, remove the `addressesByName` construction, the duplicate-address `FindDuplicateNames` call, and `ValidateAddressDefinitions`; change `ValidateTarget` to look the exchange up directly from `exchangesByName` and emit an "unknown exchange" error when absent (replacing the current "references unknown address" message), then run the existing `ValidateTargetAgainstExchange` against it. `ValidateTargets`/`ValidateTarget` signatures lose the `addressesByName` parameter. `GetTargetDescription` and any other user-facing strings that mention "address" are reworded to "exchange"; update the corresponding test assertions accordingly.

**Tests.** Update the affected unit tests (`AddRabbitMqPublishTopologyTests`, `AddRabbitMqConsumeTopologyTests`, `RabbitMqChannelGroupTests`) and integration tests (`RabbitMqPublishingIntegrationTests`, `RabbitMqDedicatedTopologiesIntegrationTests`) to declare targets against exchanges directly. Any test asserting address-resolution or address-validation behavior is removed or rewritten as the equivalent exchange-reference / unknown-exchange assertion. No new test infrastructure is required.
