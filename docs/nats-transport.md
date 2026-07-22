# NATS transport

`BrilliantMessaging.Transport.Nats` adds JetStream-backed NATS messaging. It does not implement core NATS
pub/sub; all configured streams, consumers, publishing, acknowledgements, retries, and dead-letter behavior use
JetStream.

## Setup

Install the package:

```bash
dotnet add package BrilliantMessaging.Transport.Nats
```

Declare a NATS topology with a server URI, at least one stream, outbound subjects, and durable consumers:

```csharp
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Transport.Nats;

builder
    .Services
    .AddBrilliantMessaging()
    .UseCloudEvents(options => options.Source = "/shop/orders")
    .MapMessageContracts(contracts =>
        contracts.Map<OrderPlaced>("shop.order.placed"))
    .AddNatsTopology(nats =>
    {
        nats.UseServer("nats://localhost:4222");

        nats.Stream("ORDERS", stream => stream
            .Subject("orders.*")
            .Storage(NatsStreamStorage.File)
            .Retention(NatsStreamRetention.Limits)
            .Replicas(1)
            .DuplicateWindow(TimeSpan.FromMinutes(2)));

        nats.Publish<OrderPlaced>(target => target
            .ToSubject("orders.placed")
            .UseMessageIdDeduplication());

        nats.Consume("ORDERS", "orders-worker", consumer => consumer
            .FilterSubject("orders.placed")
            .AckWait(TimeSpan.FromSeconds(30))
            .MaxDeliver(10)
            .DeadLetterAfterDeliveryAttempt(5)
            .MaxAckPending(1024)
            .DeadLetterSubject("orders.dead")
            .Handle<OrderPlaced, OrderPlacedHandler>());
    });
```

Use `UseOptions(NatsOpts)` or `UseOptions(IServiceProvider => NatsOpts)` when you need credentials, TLS,
custom reconnect settings, or any other `NATS.Net` connection option.

## Topology

Streams declare explicit subject patterns. Stream patterns may use NATS wildcards such as `*` and `>`.
Outbound targets use literal subjects through `ToSubject(...)`; wildcards are rejected for publish subjects. All
subjects reject whitespace and control characters before the transport connects to NATS.

Streams configure storage, retention, replicas, and duplicate windows. Consumers are pull-based durable JetStream
consumers. A consumer references a stream and durable name, can set an optional filter subject pattern, and can
configure `AckWait`, `MaxDeliver`, `DeadLetterAfterDeliveryAttempt`, `MaxAckPending`, and `MaxBufferedMessages`.
Filter subjects may use `*` and `>` wildcards and must overlap at least one subject pattern declared by the
referenced stream. Stream and durable names must not contain whitespace, control characters, `.`, `*`, `>`, `/`,
or `\`. Stream replica counts must be between one and five.

Choose stream retention according to its delivery semantics:

- `Limits` retains messages until a configured age, count, or size limit removes them.
- `Interest` retains messages only while matching consumers have interest. Provision consumers before publishing;
  a message with no matching consumer is deleted immediately.
- `WorkQueue` delivers each message to one consumer. Distinct consumers on the stream must use disjoint filters;
  multiple workers processing the same work partition should share one durable consumer.

Configured stream limits still apply to `Interest` and `WorkQueue` retention and can remove messages before they are
processed.

Brilliant Messaging provisions streams and durable consumers by default. Use
`Provisioning(NatsTopologyProvisioningMode.AssertOnly)` when JetStream infrastructure is managed externally and
the application should only assert that resources exist.

## Publishing

Publishing goes through the normal Brilliant Messaging path: message contracts, CloudEvents metadata,
serialization, trace context, outbound diagnostics, and the transport target. The NATS target publishes
CloudEvents binary content mode over NATS headers using valid `ce-` wire headers, then waits for the JetStream
publish acknowledgement.

Targets can be unnamed default targets or named targets via `PublishNamed<TMessage>(...)`. Target-level
serializer overrides are supported with `WithSerializer<TSerializer>()`.

Deduplication is off by default. `UseMessageIdDeduplication()` sets the JetStream message id to the CloudEvents
`id`. Effective once-only publish acceptance also requires a stream duplicate window that covers the subject and
the expected retry window.

## Consuming

Consumed messages flow through the normal inbound path: CloudEvents inspection, trace context, deserialization
middleware, acknowledgement middleware, handler invocation, and inbound diagnostics. Inbound header names are
matched case-insensitively, so external producers may use canonical HTTP-style casing (`Content-Type`,
`Ce-Type`); CloudEvents attribute names are canonicalized to their lowercase spec form during mapping. Handler-level
deserializer overrides, `HandleNamed`, manual acknowledgement, and handler-level acknowledgement options are
available.

Successful automatic handler completion sends JetStream `Ack`. Retryable handler failure sends `Nak` with a
delay. `RetryMessageException` is classified as retryable; `RejectMessageException` is classified as rejected.
Rejected messages and retry exhaustion publish the original payload and headers to the configured dead-letter
subject, wait for the JetStream publish acknowledgement, and then terminate the original message. Messages
that cannot enter the pipeline at all — an unregistered CloudEvents type, no matching handler, or a malformed
CloudEvents envelope — take the same dead-letter-then-terminate path immediately, without consuming retries. The
dead-letter copy is published under a message id derived from the original id (or the stream sequence), the
durable consumer name, and the dead-letter subject, so a stream-wide duplicate window neither suppresses the
copy as a duplicate of the original nor stores a second copy when a retried delivery repeats the dead-letter
publish. The original CloudEvents id remains available on the copy via the `ce-id` header.

The originating consumer must not select its own dead-letter subject. When one stream captures both the live and
dead-letter subjects, configure `FilterSubject(...)` so the durable excludes the dead-letter subject. An unfiltered
originating consumer requires the dead-letter subject to be captured by a different declared stream; topology
compilation rejects configurations that would feed dead-letter copies back into the same consumer.

## Reliability

Consumption is at-least-once. Handlers should be idempotent. Ordering is not guaranteed once concurrent
consumers, retries, delayed redelivery, or dead-letter routing are involved.

Long-running handlers are kept in-flight by periodic JetStream `AckProgress` while the handler runs. This is on
by default and can be disabled with `AckProgress(false)`. If it is disabled, size `AckWait` to cover the slowest
handler. `AckWait` must be at least 3 seconds: the heartbeat runs every `AckWait / 3`, and shorter windows could
not be kept in flight reliably.

`MaxDeliver` is provisioned exactly on the JetStream consumer and is the absolute server-side delivery limit.
`DeadLetterAfterDeliveryAttempt` is the client-side delivery ordinal on which a normally failed delivery is
dead-lettered or terminated. Its default is 5, while `MaxDeliver` defaults to 10, reserving five additional server
deliveries as shutdown-interruption headroom. Configure the values independently when a different balance is
needed; `DeadLetterAfterDeliveryAttempt` must not exceed `MaxDeliver`.

Both settings use JetStream's `NumDelivered`, which counts every delivery regardless of why it was redelivered.
They are not durable handler-failure counters: shutdown interruptions, acknowledgement timeouts, and process exits
advance the delivery ordinal and can therefore reduce the remaining normal retry window and increase subsequent
retry backoff.

Deliveries interrupted by graceful shutdown are NAK'd with a short delay and bypass the normal retry backoff and
client-side dead-letter threshold for that interruption, so another or restarted instance can pick them up
promptly instead of waiting out `AckWait`. The transport continues doing this while the delivery ordinal is below
`MaxDeliver`. When an interruption reaches the server limit, the message is dead-lettered with a distinct terminate
reason rather than NAK'd again.

That final-attempt handling requires the transport to receive and settle the delivery. If the process exits or
otherwise fails to settle the final server attempt, JetStream reaches its server-side `MaxDeliver`, stops
redelivery, and leaves the message stored. A retained `WorkQueue` message must then be removed manually through
the JetStream API. Monitor JetStream maximum-delivery advisories so these messages receive operational attention.

Each consumer worker pulls up to `MaxBufferedMessages` messages (default 8) into a client-side buffer and
dispatches them sequentially. Only the message currently in a handler is heartbeated; buffered messages keep
counting against `AckWait` from the moment the server delivered them. Size the buffer so that
`MaxBufferedMessages × worst-case handler duration` stays well below `AckWait`, otherwise buffered messages are
redelivered while their first delivery is still queued, causing duplicate processing. Larger buffers only hide
fetch round-trip latency; they do not increase processing throughput — use `Concurrency` for that. The total
client-side buffer per consumer is `Concurrency × MaxBufferedMessages`, and the server additionally caps
outstanding deliveries at `MaxAckPending`.

NATS server maximum payload is 1 MB by default. Increase the server limit and stream policy deliberately before
publishing larger messages.

Connection resilience is delegated to `NATS.Net` built-in reconnection. The transport does not implement a
separate reconnect loop.

## Non-goal

Core NATS pub/sub is intentionally out of scope. The package name leaves room for a future no-guarantees core
NATS mode, but this transport's current reliability contract depends on JetStream streams, publish
acknowledgements, durable consumers, explicit acknowledgements, and redelivery semantics.
