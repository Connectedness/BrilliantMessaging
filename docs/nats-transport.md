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
            .MaxDeliver(5)
            .MaxAckPending(1024)
            .DeadLetterSubject("orders.dead")
            .Handle<OrderPlaced, OrderPlacedHandler>());
    });
```

Use `UseOptions(NatsOpts)` or `UseOptions(IServiceProvider => NatsOpts)` when you need credentials, TLS,
custom reconnect settings, or any other `NATS.Net` connection option.

## Topology

Streams declare explicit subject patterns. Stream patterns may use NATS wildcards such as `*` and `>`.
Outbound targets use literal subjects through `ToSubject(...)`; wildcards are rejected for publish subjects.

Streams configure storage, retention, replicas, and duplicate windows. Consumers are pull-based durable JetStream
consumers. A consumer references a stream and durable name, can set an optional filter subject, and can configure
`AckWait`, `MaxDeliver`, `MaxAckPending`, and `MaxBufferedMessages`.

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
middleware, acknowledgement middleware, handler invocation, and inbound diagnostics. Handler-level
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

## Reliability

Consumption is at-least-once. Handlers should be idempotent. Ordering is not guaranteed once concurrent
consumers, retries, delayed redelivery, or dead-letter routing are involved.

Long-running handlers are kept in-flight by periodic JetStream `AckProgress` while the handler runs. This is on
by default and can be disabled with `AckProgress(false)`. If it is disabled, size `AckWait` to cover the slowest
handler.

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
