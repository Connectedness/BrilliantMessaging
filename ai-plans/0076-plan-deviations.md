# 0076 — Plan Deviations

This file records the changes made **after** the initial implementation of
[`0076-add-nats-transport.md`](./0076-add-nats-transport.md). They came out of review passes over the transport —
several driven by tests written specifically to expose reliability gaps (`c636199`) — and are documented here
because they refine, and in places correct, the behaviour the plan described. This is not a plan; it is a change
log for the follow-up commits.

The acceptance criteria in the plan remain satisfied; these deviations adjust *how* several of them are met, and
add public configuration knobs the plan did not anticipate.

---

## 1. `NATS.Net` 3.0.0 instead of 2.8.2 (`4191384`)

**What the plan described.** The technical details named `NATS.Net` 2.8.2 and noted that it supports
`netstandard2.0`, matching the existing transports' target framework.

**Change.** The transport references `NATS.Net` 3.0.0. The `netstandard2.0` premise still holds, so the target
framework is unchanged and the reason the plan gave for the version note remains satisfied.

**Files.** `Directory.Packages.props`.

---

## 2. Server delivery ceiling and client dead-letter threshold are separate knobs (`1a7c189`, `1fcb013`)

**What the plan described.** A single `MaxDeliver` policy knob, with retry exhaustion routing the message to the
dead-letter destination.

**Change.** One value could not describe both limits honestly, because JetStream's `NumDelivered` counts *every*
delivery — including ones interrupted by shutdown or an acknowledgement timeout, which are not handler failures.
The transport now exposes two knobs:

- `MaxDeliver` — provisioned exactly on the JetStream consumer; the absolute server-side ceiling. Default 10.
- `DeadLetterAfterDeliveryAttempt` — the client-side delivery ordinal on which a normally failed delivery is
  dead-lettered or terminated. Default 5.

The gap between the defaults is deliberate headroom for shutdown interruptions (see §3). Validation rejects a
`DeadLetterAfterDeliveryAttempt` greater than `MaxDeliver`.

An intermediate revision (`1a7c189`) provisioned the consumer with twice the configured `MaxDeliver` implicitly;
`1fcb013` replaced that with the explicit two-knob model so the server ceiling is never silently different from
what was configured.

**Files.** `Inbound/NatsInboundConsumerBuilder.cs`, `Inbound/NatsMessageAcknowledgement.cs`,
`NatsTopologyCompiler.cs`, `NatsTopologyProvisioner.cs`, `docs/nats-transport.md`.

---

## 3. Shutdown-interrupted deliveries take a distinct settlement path (`379d5ff`, `1a7c189`)

**What the plan described.** Nothing — the plan mapped handler outcomes to JetStream primitives but did not
address deliveries cancelled mid-flight by graceful shutdown.

**Change.** Treating an interruption as a handler failure was actively wrong: it consumed a retry backoff, and on
the final attempt dead-lettered a message that never failed. Interrupted deliveries now NAK with a short fixed
delay (`ShutdownRequeueDelay`, 1s), bypassing the retry backoff and the client-side dead-letter threshold, so a
surviving or restarted instance picks them up promptly instead of waiting out `AckWait`. The delay (rather than an
undelayed NAK) keeps the redelivery from landing back in the stopping instance's still-draining pull buffer.

This required the acknowledgement to receive the runtime's stopping token, because the framework acknowledgement
middleware settles auto-ack deliveries before the runtime's own requeue would run. Once interruptions exhaust the
server-side headroom the message is dead-lettered with a distinct terminate reason rather than NAK'd again, which
would strand it.

**Files.** `Inbound/NatsMessageAcknowledgement.cs`, `NatsTopologyRuntime.cs`.

---

## 4. Dead-letter copies publish under a derived message id (`b702ccb`)

**What the plan described.** Dead-letter routing republishes to the configured subject, awaits the publish
acknowledgement, and only then settles the original.

**Change.** That sequence deadlocked against the transport's own deduplication guidance. Republishing the
original's `Nats-Msg-Id` inside the stream's duplicate window made JetStream drop the copy and fail the publish
acknowledgement, so the original was never terminated and became stranded once `MaxDeliver` was exhausted — in
exactly the single-stream configuration the documentation recommends.

The copy is now published under an id derived from the original id (or the stream sequence, when the producer set
none), the durable consumer name, and the dead-letter subject. A duplicate acknowledgement for that derived id is
treated as success: it proves an earlier publish already stored the copy, which keeps the
dead-letter-then-terminate sequence idempotent when a failed terminate causes a redelivery to retry it. The
original CloudEvents id remains available on the copy via the `ce-id` header.

**Files.** `NatsTopologyRuntime.cs`, `docs/nats-transport.md`.

---

## 5. `AckProgress` uses per-consumer slot polling, and `AckWait` has a floor (`8448c80`, `4100441`, `6bf81a1`)

**What the plan described.** The transport periodically sends `AckProgress` heartbeats for in-flight messages on a
timer derived from `AckWait`.

**Change.** The behaviour is as planned; the mechanism is not. A per-message timer allocated a linked CTS, a
`Task.Run` state machine, a closure, a timer entry, and a lease for every delivery — all built and torn down
without ever firing for any handler faster than the heartbeat interval, while the closure pinned the message body.

Because workers are strictly sequential, one in-flight slot per worker suffices: dispatch performs two volatile
writes, and a single per-consumer loop heartbeats whatever occupies the slots on an `AckWait / 3` interval.

This introduced a validation the plan did not call for: `AckWait` must be at least 3 seconds, since the heartbeat
interval is derived from it and shorter windows cannot be kept in flight reliably.

**Files.** `NatsTopologyRuntime.cs`, `NatsTopologyCompiler.cs`, `docs/nats-transport.md`.

---

## 6. New `MaxBufferedMessages` consumer knob (`b6ae97f`)

**What the plan described.** Nothing — client-side pull buffering was not addressed.

**Change.** Each worker pulled up to 512 messages, but only the in-flight one is heartbeated; the buffered tail
kept counting against `AckWait` and was redelivered behind slow handlers, causing duplicate processing. The
hardcoded buffer is now a per-consumer `MaxBufferedMessages` knob (default 8), with documented guidance to size
`MaxBufferedMessages × worst-case handler duration` well below `AckWait`, and a note that larger buffers only hide
fetch round-trip latency — `Concurrency` is the throughput knob.

**Files.** `Inbound/NatsInboundConsumerBuilder.cs`, `NatsTopologyRuntime.cs`, `docs/nats-transport.md`.

---

## 7. Deliveries that cannot enter the pipeline are dead-lettered immediately (`242b315`, `42cfac3`)

**What the plan described.** Topology validation catches configuration problems; the runtime path assumed
messages that reach a consumer are dispatchable.

**Change.** Only `UnknownInboundMessageException` was caught during inspection, so a known `ce-type` with a missing
required attribute or a malformed timestamp escaped to the worker loop, which logged and continued *without
settling the delivery*. JetStream redelivered until `MaxDeliver` and then dropped the message with no dead-letter
copy — invisible data loss, easiest to trigger from an external producer.

Inspection is pure computation over received headers, so any failure is deterministic malformation and redelivery
can never succeed. All non-cancellation inspection failures — plus unroutable messages with no matching handler —
now take the immediate dead-letter-then-terminate path without consuming retries. The failure is logged with the
actual exception and classified through `ResolveErrorType` for the consumed-messages metric, since this
pre-pipeline path never reaches `InboundDiagnosticsMiddleware`.

Inbound contracts are also resolved against the topology's effective registry rather than the DI singleton, so
topology-local contract dialects are honoured (`42cfac3`).

**Files.** `NatsTopologyRuntime.cs`.

---

## 8. Topology validation beyond the plan's list (`cedc7ba`, `12e3943`, `897aa01`, `5c07498`, `c55ccf9`)

**What the plan described.** Validation of duplicate streams or consumers, invalid or blank subjects, missing
stream references, consumers without handlers, dead-letter subjects not covered by a stream, invalid policy
values, and assert-only mismatches.

**Change.** All of the above ship, plus these, each closing a configuration that compiles cleanly but misbehaves
at runtime:

- A consumer must not select its own dead-letter subject, which would feed dead-letter copies back into the
  consumer that produced them (`cedc7ba`).
- Distinct consumers on a `WorkQueue` stream must use disjoint filters, which JetStream itself requires
  (`12e3943`).
- A consumer's wildcard filter subject must overlap at least one subject pattern declared by its stream
  (`897aa01`).
- Stream and durable names are validated against JetStream's naming rules, and replica counts constrained to
  1–5 (`5c07498`).
- Assert-only mode validates `AckPolicy` and `DeliverPolicy` on existing server-side consumers, not just their
  existence (`c55ccf9`).

**Files.** `NatsTopologyCompiler.cs`, `NatsTopologyProvisioner.cs`, `docs/nats-transport.md`.
