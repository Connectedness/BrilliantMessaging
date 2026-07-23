# Add Integration Tests for Multi-Transport Scenarios

> Issue: [#83](https://github.com/Connectedness/BrilliantMessaging/issues/83)

## Rationale

Verify that RabbitMQ, NATS, and InMemory transports can be registered and run together in one application without topology, provisioning, runtime, or publishing interference. The coverage should exercise the architecture through its public APIs and hosted-service lifecycle, including a message flow that crosses all available transports.

## Acceptance Criteria

- [x] The test project `tests/Transports/BrilliantMessaging.Transports.Integration.Tests` references the RabbitMQ, NATS, and InMemory transports and is included in `BrilliantMessaging.slnx`.
- [x] Testcontainers-backed coverage registers RabbitMQ, NATS, and InMemory together in one `IServiceCollection` under distinct topology names.
- [x] An end-to-end test consumes an InMemory message, transforms and publishes it to RabbitMQ, intentionally rejects it into a RabbitMQ dead-letter queue, publishes a report from the dead-letter consumer to NATS, and verifies that a NATS subscriber receives the report.
- [x] The service provider resolves one `TopologyProvisioningHostedService` and one `TopologyRuntimeHostedService`, with `RabbitMqTopologyProvisioner` and `NatsTopologyProvisioner` contributions plus `InMemoryTopologyRuntime`, `RabbitMqTopologyRuntime`, and `NatsTopologyRuntime` contributions; starting and stopping the shared hosted services drives them all.
- [x] Registering two transports through their parameterless overloads throws `InvalidOperationException` whose message names the duplicated topology (`default`) and lists the registered topologies.
- [x] The README `Topologies` section states that topology names share one application-wide namespace across transport modules, and that multi-transport registrations must use the overloads accepting explicit, distinct names.
- [x] Tests use FluentAssertions and hand-crafted test doubles, contain no `Task.Delay`- or `Thread.Sleep`-based synchronisation, and generate unique broker resource names per run.

## Technical Details

Add a cross-transport test project under `tests/Transports` rather than placing the scenario in one transport's test assembly. Reference all three transport projects plus `Testcontainers.RabbitMq` and `Testcontainers.Nats`, add the project to `BrilliantMessaging.slnx`, and use fixtures that start RabbitMQ and JetStream-enabled NATS containers while InMemory remains process-local. Duplicate the container fixture and image-constant test support into the new project rather than extracting a shared test-support assembly, so the existing per-transport fixtures stay independent; the new fixture starts both containers under a single xUnit collection.

Build a single `ServiceCollection` through `AddBrilliantMessaging`, map the test message contracts once, and register explicitly named RabbitMQ, NATS, and InMemory topologies. Publish the initial message to InMemory and have its subscriber transform the payload into a second contract before publishing it through `IMessagePublisher.ForTopology` to the named RabbitMQ topology. Configure a RabbitMQ work queue with a dead-letter exchange and queue; its primary handler should deterministically throw `RejectMessageException` so RabbitMQ rejects the delivery without requeueing and routes it to the dead-letter queue. A second RabbitMQ handler consumes that dead-lettered message, creates a report contract, and publishes it to the named NATS topology. The final NATS subscriber records the report through an asynchronous probe.

Start the resolved `IHostedService` instances in registration order and stop them in reverse order so the test exercises `TopologyProvisioningHostedService` and `TopologyRuntimeHostedService` exactly as an application host would. Assert the transformed message and final report contents so the test proves each handler participated in the chain. Use bounded cancellation and probe completion instead of retry exhaustion, fixed delays, or another timing-dependent failure mechanism.

Assert the registered topology names, one RabbitMQ and one NATS `ITopologyProvisioner`, and one InMemory, RabbitMQ, and NATS `ITopologyRuntime` before completing the end-to-end flow. This makes failures attributable to aggregation or lifecycle wiring even when message delivery happens to succeed. Keep broker resource names unique per test so parallel or repeated runs do not share state.

Add focused coverage for the shared `TopologyRegistrationCatalog` behavior by combining parameterless transport registration overloads and asserting the duplicate `Topology.DefaultName` diagnostic. Update the README topology guidance to state that topology names share one application-wide namespace across transport modules and that multi-transport registrations must use the overloads accepting explicit, distinct names. Production behavior changes are out of scope. A failing end-to-end assertion nevertheless blocks this PR — the multi-transport flow is the deliverable, not an aspiration — so a defect exposed here is fixed in a follow-up issue that this work waits on rather than being skipped or asserted away. Only broker or container flakiness demonstrably unrelated to Brilliant Messaging may be deferred without blocking.
