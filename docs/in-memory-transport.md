# In-memory transport

`BrilliantMessaging.Transport.InMemory` is a supported, process-local transport for tests, samples,
local development, and saga or workflow experiments. It does not talk to another process and it does
not persist messages. Broker state is stored in the service provider that owns the topology and is
discarded with that provider.

The transport still exercises the normal Brilliant Messaging path:

- `IMessagePublisher` target resolution
- message serialization and CloudEvents binary content-mode headers
- inbound CloudEvents inspection and deserialization
- inbound middleware and diagnostics
- acknowledgement, retry, dead-letter, drain, and shutdown behavior

## Registration

Use `AddInMemoryTopology` when publishers and consumers should share a single process-local broker and
communicate in process. `AddInMemoryOutboundTopology` and `AddInMemoryInboundTopology` register
direction-specific topologies, but each topology owns its own in-memory broker; the default outbound and
inbound topologies are isolated and do not exchange messages with each other.

```csharp
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Transport.InMemory;

builder
    .Services
    .AddBrilliantMessaging()
    .UseCloudEvents(options => options.Source = "/orders")
    .MapMessageContracts(contracts =>
        contracts.Map<OrderPlaced>("orders.placed"))
    .AddInMemoryTopology(memory =>
    {
        memory.Topic("orders");
        memory.Topic("orders.dead");

        memory.Publish<OrderPlaced>(target =>
            target.ToTopic("orders"));

        memory.Consume("orders", consumer => consumer
            .Concurrency(1)
            .OnFailure(failure => failure
                .Retry(retry => retry
                    .MaxAttempts(3)
                    .ExponentialBackoff(TimeSpan.FromMilliseconds(25)))
                .DeadLetterTo("orders.dead"))
            .Handle<OrderPlaced, OrderPlacedHandler>());
    });
```

`Topic("orders")` declares a topic resource. `ToTopic("orders")` and `Consume("orders", ...)`
reference declared topics. A topic is the only in-memory resource concept; there are no exchanges,
queues, bindings, or consumer groups.

## Builder API

`AddInMemoryTopology(name, configure)` registers a topology that can publish and consume. The overload
without `name` uses `Topology.DefaultName`.

`AddInMemoryOutboundTopology(name, configure)` registers a publish-only topology with its own broker. The
overload without `name` uses `Topology.DefaultName`.

`AddInMemoryInboundTopology(name, configure)` registers a consume-only topology with its own broker. The
overload without `name` uses `InMemoryTransportModule.DefaultInboundName`, so a default outbound topology
and default inbound topology can coexist without sharing broker state.

`InMemoryTopologyBuilder` supports:

- `Topic(string topic)` to declare a topic.
- `Publish<TMessage>(target => target.ToTopic(topic))` to route a message type to a topic.
- `Consume(topic, consumer => consumer.Handle<TMessage, THandler>())` to route topic deliveries to a handler.
- `ShutdownTimeout(TimeSpan timeout)` to control graceful runtime shutdown.

`InMemoryInboundConsumerBuilder` supports:

- `Concurrency(int workers)` to set the number of background workers for that consumer route.
- `OnFailure(...)` to configure retry and dead-letter behavior.
- `Handle<TMessage, THandler>()` and `HandleNamed<TMessage, THandler>(name)` to bind handlers.

`InMemoryDeliveryPolicyBuilder` supports:

- `Retry(retry => retry.MaxAttempts(n).LinearBackoff(delay))`
- `Retry(retry => retry.MaxAttempts(n).ExponentialBackoff(delay))`
- `DeadLetterTo(topic)`

`MaxAttempts` counts total delivery attempts, including the initial delivery.

## Routing

Publishing is asynchronous with respect to handlers. A publish records the serialized message and queues
a delivery for each consumer route subscribed to the topic; handlers are invoked by background workers,
not inline during publish.

When several consumers call `Consume` for the same topic, each consumer route receives a fanout copy of
every published message. Competing consumer groups are intentionally out of scope.

Each consumer route preserves FIFO delivery by default because `Concurrency` defaults to `1`. Raising
`Concurrency` lets multiple background workers process that route in parallel and relaxes strict ordering.

## Failure Handling

The default failure behavior is drop. A handler exception, `RetryMessageException`, or unrecognized
message is settled without requeue unless the consumer has an explicit retry policy.

With `Retry(...)` configured, ordinary handler failures and `RetryMessageException` schedule another
attempt until `MaxAttempts` is reached. `RetryMessageException` does not force retry when no retry
policy exists.

`RejectMessageException` is poison: it bypasses retry and goes straight to dead-letter handling.

When attempts are exhausted or a delivery is rejected, `DeadLetterTo(topic)` republishes the serialized
delivery to the named topic. If `DeadLetterTo` is not configured, the delivery is dropped. Dead-letter
inspection is just normal topic inspection on the dead-letter topic; there is no separate dead-letter
store.

Retry delays are scheduled through `IInMemoryDelayScheduler`. The default implementation,
`RealTimeInMemoryDelayScheduler`, uses real time. Tests can replace it with the shipped
`ManualDelayScheduler` to drive retries without sleeping — see [Test helpers](#test-helpers).

## Support API

`InMemoryBroker` is a public support service for tests and local inspection:

```csharp
var broker = serviceProvider.GetRequiredService<InMemoryBroker>();

IReadOnlyList<InMemoryTransportMessage> orders = broker.GetMessages("orders");
broker.ClearRecordings("orders");
await broker.DrainUntilIdleAsync(TimeSpan.FromSeconds(5), cancellationToken);
```

`GetMessages(string topic)` returns every message routed to the topic in routing order, including
messages that were consumed. The returned `InMemoryTransportMessage` exposes the serialized body,
headers, content type, message id, topic, and delivery attempt metadata.

Recording is controlled at the topology level. Pick one of the following modes — the calls are
alternatives, not steps (a later call overrides an earlier one):

```csharp
builder.AddInMemoryTopology(memory =>
{
    memory.RecordMessages();                 // default: record every routed message
    memory.RecordMessages(false);            // disable recording
    memory.RecordMessages(maxPerTopic: 100); // keep the newest 100 per topic
});
```

- `RecordMessages()` is the default. It records every routed message without a per-topic bound, so
  `GetMessages(topic)` can be used for exact-count assertions in short-lived tests.
- `RecordMessages(false)` disables recording. `GetMessages(topic)` returns an empty list and the broker
  does not create per-topic recording state.
- `RecordMessages(maxPerTopic: N)` keeps only the most recent `N` recorded messages per topic. When the
  cap is reached, older recordings are evicted as newer messages arrive.

Bounded recording is explicitly truncating: `GetMessages(topic)` returns only the most recent `N`
messages for that topic, so exact-count assertions across more than `N` routed messages will not hold.
Under concurrent routing, a topic may briefly exceed the cap before trimming settles.

Recordings accumulate for the lifetime of the broker unless recording is disabled, bounded, or cleared.
This is intentional so tests can inspect everything that was ever routed, but it means a long-running
local-development host backed by the in-memory transport should disable recording, use bounded
recording, or periodically clear recordings to avoid retaining every routed message in memory.

`ClearRecordings()` removes recordings for all topics. `ClearRecordings(string topic)` removes
recordings for one topic; clearing an unrecorded topic is a no-op.

For named topologies, resolve the broker by topology key:

```csharp
var broker = serviceProvider.GetRequiredKeyedService<InMemoryBroker>("orders");
```

The default registration isolates broker state per service provider. Publishers, runtimes, and support
APIs inside one provider share the same broker; a different provider gets a different broker.

## Drain Behavior

`DrainUntilIdleAsync(timeout, cancellationToken)` waits until there are no queued deliveries, no active
handler invocations, and no retry deliveries that have already been scheduled. It throws
`TimeoutException` if the broker does not become idle before the timeout, and it observes cancellation.

This is intended for deterministic tests:

```csharp
await publisher.PublishMessageAsync(new OrderPlaced("42"), cancellationToken: cancellationToken);
await broker.DrainUntilIdleAsync(TimeSpan.FromSeconds(5), cancellationToken);

broker.GetMessages("orders.dead").Should().ContainSingle();
```

## Shutdown

Inbound topologies are driven by the normal `TopologyRuntimeHostedService` and `ITopologyRuntime`
start/stop model. On stop, the broker:

- stops accepting newly published work;
- waits for queued deliveries, in-flight handlers, and already scheduled retry deliveries to drain until
  the topology `ShutdownTimeout`;
- cancels remaining in-flight or scheduled work after the timeout; and
- closes the background worker queues.

Publishing after the runtime has stopped fails with an `InvalidOperationException`.

## Test helpers

The package ships a few helpers in the `BrilliantMessaging.Transport.InMemory` namespace so you do not
have to reimplement them in every test project. They are intended for tests, samples, and local
development; production code does not need them.

### InMemoryTestHost

`InMemoryTestHost` wires Brilliant Messaging with an in-memory topology, starts the topology runtimes,
and exposes the publisher and broker. It is an `IAsyncDisposable`: disposing it stops the runtimes and
disposes the underlying service provider.

```csharp
await using var host = await InMemoryTestHost.StartAsync(builder => builder
    .MapMessageContracts(contracts => contracts.Map<OrderPlaced>("orders.placed"))
    .AddInMemoryTopology(memory =>
    {
        memory.Topic("orders");
        memory.Publish<OrderPlaced>(target => target.ToTopic("orders"));
        memory.Consume("orders", consumer => consumer.Handle<OrderPlaced, OrderPlacedHandler>());
    }));

await host.Publisher.PublishMessageAsync(new OrderPlaced("42"), cancellationToken: cancellationToken);
await host.Broker.DrainUntilIdleAsync(TimeSpan.FromSeconds(5), cancellationToken);

host.Broker.GetMessages("orders").Should().ContainSingle();
```

- `StartAsync(configure, scheduler = null, source = "/in-memory")` runs `configure` against a fresh
  `BrilliantMessagingBuilder`. `scheduler` overrides the delay scheduler (see `ManualDelayScheduler`
  below); `source` sets the CloudEvents source applied to published messages.
- `Publisher`, `Broker`, `BrokerFor(name)`, and `Topology` resolve the wired services.
- `Probe` returns the registered `HandlerProbe`, or `null` when none was registered.
- `StopRuntimesAsync()` stops the runtimes without disposing the container, so a test can still resolve
  services and assert post-shutdown behavior.

### HandlerProbe

`HandlerProbe` is a shared recording handler helper. Register it as a singleton and forward your handlers
to it, then assert on what was delivered, force failures, or hold deliveries in flight.

```csharp
services.AddSingleton<HandlerProbe>();

public sealed class OrderPlacedHandler(HandlerProbe probe) : IMessageHandler<OrderPlaced>
{
    public Task HandleAsync(OrderPlaced message, IncomingMessageContext context, CancellationToken cancellationToken) =>
        probe.HandleAsync(
            context.Transport.Source,
            context.Endpoint.Name,
            message,
            context.Transport.DeliveryAttempt,
            cancellationToken);
}
```

```csharp
// Fail the first two attempts, then succeed.
host.Probe!.OnHandle = invocation =>
    invocation.DeliveryAttempt < 3 ? new InvalidOperationException("transient") : null;

await host.Probe.WaitForInvocationsAsync(3, TimeSpan.FromSeconds(5));
host.Probe.Invocations.Should().HaveCount(3);
```

- `Invocations` exposes the recorded `HandlerInvocation`s in order (route, endpoint name, message, and
  delivery attempt).
- `OnHandle` returns the exception to throw for an invocation, or `null` to complete successfully.
- `Gate`, when set, makes every invocation await the supplied `TaskCompletionSource` before returning,
  letting a test hold a delivery in flight.
- `WaitForInvocationsAsync(count, timeout)` completes once `count` invocations are recorded and throws
  `TimeoutException` otherwise.

### ManualDelayScheduler

`ManualDelayScheduler` is a deterministic `IInMemoryDelayScheduler` that captures retry delays instead of
sleeping, so retry and backoff tests run without real time. Register it as the scheduler — it wins over
the default `RealTimeInMemoryDelayScheduler`, which is registered with `TryAdd`:

```csharp
var scheduler = new ManualDelayScheduler();

// Through the host:
await using var host = await InMemoryTestHost.StartAsync(builder => /* ... */, scheduler);

// Or directly, before AddInMemoryTopology runs:
services.AddSingleton<IInMemoryDelayScheduler>(scheduler);
```

```csharp
await scheduler.WaitForPendingAsync(1, TimeSpan.FromSeconds(5)); // wait until a retry is scheduled
scheduler.ReleaseAll();                                          // let the scheduled retries run
scheduler.RequestedDelays.Should().Equal(TimeSpan.FromMilliseconds(25));
```

- `PendingCount` and `RequestedDelays` observe the delays captured so far.
- `ReleaseAll()` completes every captured delay immediately.
- `WaitForPendingAsync(count, timeout)` waits until `count` delays are captured and throws
  `TimeoutException` otherwise.
