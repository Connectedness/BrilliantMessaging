<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="design/hero-dark.svg" />
    <img alt="Brilliant Messaging - Explicit. No Magic." src="design/hero-light.svg" width="420" />
  </picture>
</p>

<div align="center">

[![License](https://img.shields.io/badge/License-MIT-green.svg?style=for-the-badge)](https://github.com/Connectedness/BMF/blob/main/LICENSE)
[![NuGet](https://img.shields.io/nuget/v/BrilliantMessaging.Core?style=for-the-badge&color=blue)](https://www.nuget.org/packages/BrilliantMessaging.Core)
[![Documentation](https://img.shields.io/badge/Docs-Changelog-yellowgreen.svg?style=for-the-badge)](https://github.com/Connectedness/BMF/releases)

</div>

Brilliant Messaging is the messaging framework that lets you keep control! No automatic, obscure generation of broker resources, no hidden dependencies, no magic. Define your topologies, publish messages, and subscribe to them. Promotes CloudEvents. That's it!

## Why Brilliant Messaging?

Most messaging libraries try to be helpful by guessing what broker resources you want and conjuring them into existence at startup. That convenience becomes a liability the first time a typo silently provisions a phantom queue in production. Brilliant Messaging takes the opposite stance:

- **You declare, Brilliant Messaging provisions — nothing more.** Every exchange, queue, and binding is something you wrote down. There are no surprise resources on your broker.
- **CloudEvents are first-class, not bolted on.** Messages travel as [CloudEvents v1.0](https://cloudevents.io/) in *binary* content mode over AMQP 0.9.1. Interop is the default, not a serializer you have to remember to configure.
- **It lives inside the .NET host.** Configuration is a fluent chain off `IServiceCollection`; the runtime is driven by hosted services. If you know `Microsoft.Extensions.DependencyInjection` and `IHost`, you already know where Brilliant Messaging fits.
- **The whole API is yours.** Brilliant Messaging prefers `public` over `internal` — the extension points it uses internally are the same ones you can reach for. ([Public types, hidden in plain sight.](https://blog.ploeh.dk/2015/09/21/public-types-hidden-in-plain-sight/))

## Packages

All packages target `netstandard2.0`, so they happily light up on modern .NET as well as older runtimes.

| Package                                 | What it gives you                                                                                           |
|-----------------------------------------|-------------------------------------------------------------------------------------------------------------|
| `BrilliantMessaging.Abstractions`       | The CloudEvents contracts: `ICloudEvent` and `BaseCloudEvent`.                                              |
| `BrilliantMessaging.Core`               | Publishing, consuming, message contracts, the topology model, and DI wiring.                                |
| `BrilliantMessaging.Transport.InMemory` | A process-local, non-durable transport for tests, samples, local development, and workflow experimentation. |
| `BrilliantMessaging.Transport.Nats`     | The NATS transport for JetStream-backed streams, durable consumers, and subject publishing.                 |
| `BrilliantMessaging.Transport.RabbitMq` | The RabbitMQ transport — exchanges, queues, bindings, publishers, and consumers.                            |
| `BrilliantMessaging.OpenTelemetry`      | One-line registration of Brilliant Messaging tracing and metrics with the OpenTelemetry SDK.                |

## Installation

Choose the transport that matches the runtime you want. RabbitMQ is the distributed broker transport:

```bash
dotnet add package BrilliantMessaging.Transport.RabbitMq
```

The in-memory transport is useful for tests and local process-only scenarios:

```bash
dotnet add package BrilliantMessaging.Transport.InMemory
```

Use NATS when you want JetStream streams and durable consumers:

```bash
dotnet add package BrilliantMessaging.Transport.Nats
```

Want distributed traces and metrics? Add the optional observability integration
alongside it — see [Observability](#observability):

```bash
dotnet add package BrilliantMessaging.OpenTelemetry
```

## In-memory transport

`BrilliantMessaging.Transport.InMemory` runs the real Brilliant Messaging publish, serialization,
CloudEvents, inbound middleware, acknowledgement, retry, diagnostics, and shutdown path without an
external broker. It is process-local and non-durable: messages never leave the current service provider,
there is no persistence, and state is discarded when the provider is disposed.

```csharp
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Transport.InMemory;

builder
    .Services
    .AddBrilliantMessaging()
    .UseCloudEvents(options => options.Source = "/shop/orders")
    .MapMessageContracts(contracts =>
        contracts.Map<OrderPlaced>("shop.order.placed"))
    .AddInMemoryTopology(memory =>
    {
        memory.Topic("orders");
        memory.Topic("orders.dead");

        memory.Publish<OrderPlaced>(target =>
            target.ToTopic("orders"));

        memory.Consume("orders", consumer => consumer
            .OnFailure(failure => failure
                .Retry(retry => retry
                    .MaxAttempts(3)
                    .LinearBackoff(TimeSpan.FromMilliseconds(50)))
                .DeadLetterTo("orders.dead"))
            .Handle<OrderPlaced, OrderPlacedHandler>());
    });
```

For the full builder API, routing rules, retry/dead-letter behavior, broker inspection hooks,
drain semantics, and shutdown behavior, see [In-memory transport](docs/in-memory-transport.md).

## NATS transport

`BrilliantMessaging.Transport.Nats` implements JetStream-backed NATS messaging. Core NATS pub/sub is not part of
this transport: publishing waits for JetStream acknowledgement, and consuming uses pull-based durable consumers
with explicit acknowledgement.

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
            .DuplicateWindow(TimeSpan.FromMinutes(2)));

        nats.Publish<OrderPlaced>(target => target
            .ToSubject("orders.placed")
            .UseMessageIdDeduplication());

        nats.Consume("ORDERS", "orders-worker", consumer => consumer
            .FilterSubject("orders.placed")
            .DeadLetterSubject("orders.dead")
            .Handle<OrderPlaced, OrderPlacedHandler>());
    });
```

For setup, topology options, reliability semantics, deduplication, retry and dead-letter behavior, ordering, and
long-running handler caveats, see [NATS transport](docs/nats-transport.md).

## Quick start

A complete publish-and-consume loop is four small steps.

### 1. Define a message

A message *is* a CloudEvent. Inherit `BaseCloudEvent` and you get a retry-stable
`Id` (a time-ordered UUID) and `Time` for free:

```csharp
using BrilliantMessaging.Abstractions;

public sealed record OrderPlaced(string OrderId, decimal Total) : BaseCloudEvent;
```

### 2. Register Brilliant Messaging and declare a topology

Map each message type to a CloudEvents `type` discriminator, then declare exactly
the broker resources you want. Brilliant Messaging will provision these — and only these — when the
host starts.

```csharp
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Transport.RabbitMq;
using RabbitMQ.Client;

builder
    .Services
    .AddBrilliantMessaging()
    .UseCloudEvents(options => options.Source = "/shop/orders")
    .MapMessageContracts(contracts =>
        contracts.Map<OrderPlaced>("shop.order.placed"))
    .AddRabbitMqTopology(rabbit =>
    {
        rabbit.UseConnectionFactory(_ => new ConnectionFactory
        {
            Uri = new Uri("amqp://guest:guest@localhost:5672")
        });

        rabbit.Exchange("orders", ExchangeType.Topic);
        rabbit.Queue("orders-processing");
        rabbit.QueueBinding("orders", "orders-processing", "shop.order.*");

        // Outbound: where OrderPlaced goes.
        rabbit.Publish<OrderPlaced>(target =>
            target.ToTopicExchange("orders", "shop.order.placed"));

        // Inbound: who handles it.
        rabbit.Consume("orders-processing", consumer =>
            consumer.Handle<OrderPlaced, OrderPlacedHandler>());
    });
```

`AddBrilliantMessaging` wires up two hosted services that run when your `IHost` starts — no extra
`StartAsync` calls on your part:

- a **provisioning** service that declares your exchanges, queues, and bindings on
  the broker (and only those), then validates the result, and
- a **runtime** service that opens the consumers and begins delivering messages to
  your handlers.

### 3. Publish

Inject `IMessagePublisher` and send. With no explicit target, Brilliant Messaging resolves one from
the topology by message type, and fills in the CloudEvents envelope (`id`, `time`,
`source`, `type`) from the message and your configured defaults.

```csharp
using BrilliantMessaging.Core.Messaging.Outbound;

public sealed class Checkout(IMessagePublisher publisher)
{
    public Task PlaceAsync(OrderPlaced order, CancellationToken ct) =>
        publisher.PublishMessageAsync(order, cancellationToken: ct);
}
```

### 4. Handle

A handler implements `IMessageHandler<T>` and is resolved from a fresh DI scope per
delivery — so injecting scoped dependencies (a `DbContext`, say) just works.

```csharp
using BrilliantMessaging.Core.Messaging.Inbound;

public sealed class OrderPlacedHandler : IMessageHandler<OrderPlaced>
{
    public Task HandleAsync(
        OrderPlaced message,
        IncomingMessageContext context,
        CancellationToken cancellationToken)
    {
        // ... process the order ...
        return Task.CompletedTask;
    }
}
```

That's the whole loop. The rest of this document explains what each moving part is
actually doing.

## Concepts

### Messages and CloudEvents

Brilliant Messaging publishes every message as a CloudEvent v1.0 in **binary** content mode: the
CloudEvents attributes ride in the AMQP headers and your payload is the raw body.
The two attributes the *call site* owns — `Id` and `Time` — are captured when the
message object is constructed and must stay stable across retries; regenerating them
mid-flight would turn a retry into a brand-new event. `BaseCloudEvent` enforces this
for you (`Id` defaults to a time-ordered `BrilliantMessagingUuid`, `Time` to `DateTimeOffset.UtcNow`),
but you can implement `ICloudEvent` directly when you need full control. The
application-wide `source` is set once via `UseCloudEvents`, and is validated at
startup so a missing or malformed `Source` fails fast rather than at first publish.

### Message contracts

A *message contract* maps a .NET type to the CloudEvents `type` discriminator that
identifies it on the wire. This is the one piece of bookkeeping Brilliant Messaging asks of you, and
it pays off in evolution-friendliness:

```csharp
contracts
    .Map<OrderPlaced>("shop.order.placed")
    .WithDataSchema("/schemas/order-placed/v1") // optional CloudEvents dataschema
    .WithInboundAlias("orders.placed");         // also accept a legacy discriminator
```

The same registry drives serialization on the way out and type resolution on the way
in. Aliases let a consumer keep accepting an old `type` value while publishers move
to a new one — schema evolution without breaking changes.

### Topologies

A **topology** is the heart of Brilliant Messaging. It is a named bundle of broker resources
(exchanges, queues, bindings), publishing targets, and consumers — and it owns
**exactly one connection to the broker**. Everything declared inside a topology
shares that connection's lifetime.

That one-connection rule is the lever behind Brilliant Messaging's most important production advice.
The [RabbitMQ production checklist](https://www.rabbitmq.com/docs/production-checklist#apps-connection-management)
recommends separating publishing and consuming onto different connections: when a
publishing connection gets throttled by broker flow control, a *shared* connection
would stall consumer acknowledgements at exactly the moment the broker needs
consumers to drain queues. Brilliant Messaging gives you three entry points to honour this:

- `AddRabbitMqTopology` — one topology carrying both publishers and consumers over a
  single shared connection. Ideal for low-traffic services and tests.
- `AddRabbitMqOutboundTopology` + `AddRabbitMqInboundTopology` — two topologies, each
  with its own dedicated connection. The recommended shape for production services.

Topologies are named (the dedicated outbound and inbound defaults are deliberately
different so they coexist without colliding), and provisioning happens once at
startup through a hosted service. If what's on the broker doesn't match what you
declared, you'll know immediately — not three deploys later.

Topology names share one application-wide namespace across all transport modules.
When registering multiple transports in one application, use the registration
overloads that accept an explicit topology name and give every transport a distinct
name.

### Resilience and redelivery

RabbitMQ queues are declared as quorum queues by default:

```csharp
rabbit.Queue("orders-processing"); // x-queue-type = quorum
rabbit.Queue("ephemeral-work", queue => queue.AsClassicQueue());
```

This is a pre-1.0 breaking change. An existing classic queue cannot be redeclared as
quorum; RabbitMQ fails provisioning with `406 PRECONDITION_FAILED`. Either opt that
queue back into classic with `AsClassicQueue()` or migrate it intentionally (drain,
delete, redeclare).

Inbound handler failures are classified through `RedeliveryClassifier`. Quorum queues
default to retry-unless-poison: handler exceptions are settled with `requeue: true`,
while `MessageDeserializationException` and `RejectMessageException` are rejected
without requeue. `RetryMessageException` forces retry on quorum. You can narrow the
decision consumer-wide or per handler:

```csharp
rabbit
    .Consume("orders-processing", consumer => consumer
        .WithRedelivery(redelivery =>
            redelivery.ShouldRetry(ex => ex is TimeoutException)
         )
        .Handle<OrderPlaced, OrderPlacedHandler>(handler => handler
            .WithRedelivery(redelivery =>
                redelivery.ShouldRetry(ex => ex is DbUpdateConcurrencyException)
             )
         )
    );
```

Brilliant Messaging only classifies. It does not count attempts on its own;
delivery-limit tuning, delayed retry, dead-letter routing, overflow behavior, and the
other RabbitMQ policy-style queue knobs are declared on the queue builder so the
topology definition is the single Configuration-as-Code source for both resource
existence and resource policy:

```csharp
rabbit
    .Queue("orders-processing", queue => queue
        .WithDeliveryLimit(3)
        .WithDelayedRetry(TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(30))
        .WithDeadLetterStrategy(RabbitMqDeadLetterStrategy.AtMostOnce)
        .WithOverflow(RabbitMqOverflow.RejectPublish));
```

The available knob methods are `WithDeliveryLimit`, `WithDelayedRetry`,
`WithDeadLetterStrategy`, `WithOverflow`, `WithMaxPriority`, `WithQueueLeaderLocator`,
`WithInitialClusterSize`, and `WithConsumerTimeout` (alongside the existing
`WithDeadLetterExchange`, `WithMessageTtl`, `WithMaxLength`, and `WithMaxLengthBytes`).
The compiler guards queue-type-incompatible knobs: quorum-only arguments on a classic or
unknown queue, `x-overflow = reject-publish-dlx` and `x-max-priority` on a quorum queue
each hard-error with the queue name and a remediation.

The compiler auto-detects a consumer's queue type from actively declared
`x-queue-type`. For passive or externally declared queues, use
`QueueType(RabbitMqQueueType.Quorum)` only when the real broker queue is quorum:

```csharp
rabbit.Queue("orders-processing", queue => queue.WithDeclareMode(RabbitMqDeclareMode.Passive));
rabbit
    .Consume("orders-processing", consumer => consumer
    .QueueType(RabbitMqQueueType.Quorum)
    .WithRedelivery(redelivery => redelivery.ShouldRetry(_ => true))
    .Handle<OrderPlaced, OrderPlacedHandler>());
```

Classic and unknown queues default to reject-all, preserving the old one-and-done
behavior. Configuring `WithRedelivery(...)` on classic or unknown queues is a
compile-time topology error because the client cannot prove there is a broker
backstop for `requeue: true`; on these endpoints `RetryMessageException` is a no-op.
Asserting `QueueType(RabbitMqQueueType.Quorum)` for a queue that is actually classic
removes that guard and can create an unbounded redelivery loop, so use it only to
describe externally managed quorum queues accurately.

#### Evolving topology resources

RabbitMQ throws `406 PRECONDITION_FAILED` when a queue is redeclared with different
arguments, so changing a queue's policy arguments requires a new queue name rather than
redeclaring the existing one. Brilliant Messaging supports an explicit introduce → drain
→ delete workflow driven entirely from the topology definition:

1. **Introduce** a new queue (for example `orders-v2`) under a new name with the new
   arguments; keep `orders-v1` in the topology so it is re-declared unchanged. Add the
   new binding `ex → orders-v2` (`Active`) and flip the old binding
   `ex → orders-v1` to `Delete` mode so the broker unbinds it and new messages stop
   flowing to `orders-v1`. Move consumers to `orders-v2`. Deploy.
2. **Drain** `orders-v1` on the broker until empty (its binding is gone, so no new
   messages arrive).
3. **Remove** `orders-v1`: flip its declare mode to `Delete` and deploy once more — the
   framework reads the queue's ready-message count with a passive declare, fails safely
   with a clear "drain first" error if the queue is not yet empty, and otherwise deletes
   it. The delete proceeds regardless of consumers still attached to the drained queue, so
   in an init-container / rolling deployment it removes `orders-v1` even while the previous
   version's consumers are still connected (they receive a consumer-cancel and are about to
   be replaced anyway). Then remove both the old queue and old binding lines entirely.

```csharp
rabbit.Queue("orders-v2", queue => queue.WithDeliveryLimit(5));
rabbit.Queue("orders-v1", queue => queue.WithDeclareMode(RabbitMqDeclareMode.Delete));
rabbit.QueueBinding("orders", "orders-v2", "orders.created");
rabbit.QueueBinding(
    "orders",
    "orders-v1",
    "orders.created",
    binding => binding.WithBindingMode(RabbitMqBindingMode.Delete)
);
```

The provisioner runs all `Active` bindings before any `Delete` bindings, so in step 1
the new `ex → orders-v2` bind is established before the old `ex → orders-v1` unbind runs
— there is never a window in which the exchange has neither binding. A `Delete`-mode
queue or binding that is already absent on the broker succeeds silently (idempotent
restart). The empty check counts only ready messages, not messages a still-attached
consumer holds unacknowledged, so make sure the old queue has fully drained before
flipping it to `Delete` — an in-flight unacknowledged message is discarded together with
the queue. The RabbitMQ management UI remains a valid alternative for removal.

Exchanges follow the same workflow when their type or arguments must change, because
RabbitMQ refuses an in-place redeclare: a different `type` raises `530 NOT_ALLOWED`, while
different `durable`/`auto-delete`/arguments raises `406 PRECONDITION_FAILED` (both close
the channel). Introduce the replacement exchange (for example `orders-v2`) under a new
name, add the new bindings, deploy publishers so all live instances publish to
`orders-v2`, then flip the old exchange to `Delete`:

```csharp
rabbit.Exchange("orders-v2", ExchangeType.Topic);
rabbit.Exchange("orders-v1", exchange => exchange.WithDeclareMode(RabbitMqDeclareMode.Delete));
```

Exchange deletion is **unconditional**: unlike a queue, an exchange holds no messages, so
there is nothing to drain before deleting, and the broker cascade-removes the bindings
owned by the deleted exchange. The provisioner skips any binding that names a
`Delete`-mode exchange on either end, so you do not need to flip each binding to `Delete`
individually — the exchange delete cleans them up. A `Delete`-mode exchange that is
already absent on the broker succeeds silently (idempotent restart). The topology
compiler rejects an outbound target that publishes to a `Delete`-mode exchange, so the
configuration cannot accidentally route traffic to a resource that is being removed.

**Rolling-deploy hazard:** deleting an exchange breaks any older application instance
that is still publishing to it — a `basic.publish` to a missing exchange is a
`404 NOT_FOUND` channel error. The safe workflow is introduce the replacement exchange
and bindings, deploy publishers so all live instances use the replacement, **wait until
old publishers are gone**, then flip the old exchange to `Delete`. Do not flip the old
exchange to `Delete` while a previous deployment's publishers are still running against
it.

##### Headers-exchange bindings

A `headers` exchange routes on header values rather than a routing key, and the
`x-match` binding argument controls whether all configured headers must match or any one
of them. The binding builders expose a typed API for this so you do not have to reach for
the raw `WithArgument(...)` escape hatch:

```csharp
rabbit.Exchange("orders", ExchangeType.Headers);
rabbit.Queue("orders-by-tenant");
rabbit.QueueBinding(
    "orders",
    "orders-by-tenant",
    configure: binding => binding
       .WithHeaderMatch(RabbitMqHeaderMatch.All)
       .WithHeader("tenant", "acme")
       .WithHeader("region", "us")
);
```

`RabbitMqHeaderMatch` has four values: `All` and `Any` are the classic AMQP 0-9-1 modes
(logical AND / OR), while `AllWithX` and `AnyWithX` are the RabbitMQ extensions that
**include `x-`-prefixed headers in matching**. The plain `All` / `Any` modes exclude
`x-`-prefixed headers from matching. If every configured predicate is `x-`-prefixed,
plain `All` can match every message (zero effective predicates), while plain `Any`
matches none. Use `AllWithX` / `AnyWithX` when the framework's own `x-`-prefixed header
conventions (for example `x-tenant`) must participate in routing. `WithHeader(...)`
rejects the literal name `x-match` (it is the match-mode control argument, not a header
predicate) and writes a default `x-match` of `all` when none has been set yet, so a
single `WithHeader` call is unambiguous. The topology compiler rejects header-match
configuration on a binding whose source exchange is not type `headers`, rejects unknown
`x-match` values, and rejects Active headers bindings that combine plain `All` / `Any`
(or an omitted `x-match`) with `x-`-prefixed predicates.

### Publishing

`IMessagePublisher` is your outbound surface. The common call is
`PublishMessageAsync(message)`: Brilliant Messaging looks up the message type's outbound target in the
topology, builds the CloudEvent envelope, serializes the payload, and hands it to the
transport. You can supply per-call CloudEvents metadata when a single send needs to
override the defaults, or name an explicit target when a type has more than one.

### Outbound targets

An *outbound target* is the routing-and-shaping decision you attach to a message type
with `Publish<T>` (or `PublishNamed<T>` to register several under different names — handy
when the same event fans out to multiple exchanges). First, how it routes:

- `ToFanoutExchange(exchange)` — broadcast to every bound queue.
- `ToDirectExchange(exchange, routingKey)` — route by an exact key (fixed, or derived
  per message with a `Func<T, string>`).
- `ToTopicExchange(exchange, routingKey)` — route by a dot-delimited topic pattern.
- `ToHeadersExchange(exchange)` — route on header values instead of a key.

Then, how it's shaped. Each target can override the serializer with
`WithSerializer<T>()` (the default emits CloudEvents), attach fixed `WithHeader(...)`
values (which also drive routing on a headers exchange), pin itself to a named channel
group with `UseChannelGroup(...)`, and demand delivery with `Mandatory()` — see
[Publisher confirms and mandatory routing](#publisher-confirms-and-mandatory-routing).

```csharp
rabbit
    .Publish<OrderPlaced>(target =>
        target
            .ToTopicExchange("orders", "shop.order.placed")
            .WithHeader("x-tenant", "acme")
            .Mandatory()
    );
```

### Publishing through a target directly

`IMessagePublisher` is the convenient front door, but a target is itself a first-class
publishing surface — it exposes `PublishAsync(message)` and carries everything it needs
to build the envelope and route the message. Sometimes you want to skip the
type-to-target lookup and hold the target yourself: it removes a per-publish resolution,
makes the routing choice explicit at the call site, and is handy when a type has several
named targets.

You obtain targets from a `Topology`. The default topology is injectable directly;
resolve any other by name through `ITopologyRegistry`:

```csharp
public sealed class Checkout(Topology topology)
{
    public Task PlaceAsync(OrderPlaced order, CancellationToken ct) =>
        // Resolved by message type, or by name with GetRequiredTarget<OrderPlaced>("primary").
        topology.GetRequiredTarget<OrderPlaced>().PublishAsync(order, ct);
}

public sealed class Audit(ITopologyRegistry topologies)
{
    public Task RecordAsync(OrderPlaced order, CancellationToken ct)
    {
        var topology = topologies.GetRequiredTopology("audit");
        return topology.GetRequiredTarget<OrderPlaced>().PublishAsync(order, ct);
    }
}
```

`GetRequiredTarget` throws when the type or name is unknown; `TryGetTarget` is the
non-throwing counterpart.

### Routing keys

A target already knows how to route — but sometimes the *call site* knows better,
because the key is a piece of domain data (a tenant id, a partition, a country code).
For those cases you can hand a routing key in at publish time. It is a plain,
optional `string` overlaid on an already-selected target; it never selects the target
or turns into a transport-specific DSL.

The easy path is `IMessagePublisher`, which takes the key as an optional argument:

```csharp
await publisher.PublishMessageAsync(order, routingKey: order.TenantId, ct);
```

Here the key is *optional* in the truest sense: `null`, empty, or whitespace is
treated exactly as if you had omitted it, and the target falls back to its configured
routing. A non-blank key, on RabbitMQ, overrides both a fixed target routing key and a
per-message `Func<T, string>` factory (route headers stay separate, untouched).

When a routing key is non-negotiable, reach for `IOutboundRoutableTarget<T>` instead.
It is a capability only the targets that actually route on a key expose (RabbitMQ
direct and topic), and it is deliberately hidden from the routing-key-free
`OutboundTarget<T>` base — so handing a key to a fanout target is a compile error, not
a silently ignored argument. Every overload demands a non-blank key. Obtain one from
the topology:

```csharp
topology
    .GetRequiredRoutingTarget<OrderPlaced>() // or (name) for a specific target
    .PublishAsync(order, routingKey: order.TenantId, ct);
```

`GetRequiredRoutingTarget` resolves the target like `GetRequiredTarget` but additionally
throws `OutboundTargetNotRoutableException` when that target doesn't route on a
caller-supplied key.

### Consuming

Consumers are configured per queue with `Consume`, and a single queue can dispatch to
several typed handlers — Brilliant Messaging inspects each delivery's `type`, resolves the matching
`IMessageHandler<T>` from a per-delivery DI scope, and invokes it:

```csharp
rabbit
    .Consume("orders-processing", consumer => consumer
    .PrefetchCount(20)   // how many unacknowledged messages the broker may push
    .ChannelCount(4)     // spread deliveries across this many channels for parallelism
    .Handle<OrderPlaced, OrderPlacedHandler>()
    .Handle<OrderCancelled, OrderCancelledHandler>());
```

`PrefetchCount` sets the per-consumer QoS window, `ChannelCount` scales out across
channels, and `Concurrency` controls how many deliveries a single channel dispatches
in parallel. Handler types are auto-registered as scoped; register the concrete type
yourself beforehand if you want a different lifetime.

### Customizing the inbound pipeline

The journey from a raw delivery to your handler runs through three swappable stages, so
you can adapt Brilliant Messaging to non-CloudEvents producers or weave in cross-cutting concerns:

- **Inspector** — resolves a wire message to a known contract. The default reads the
  CloudEvents `type` attribute to pick the discriminator and message type. Use
  `UseInspector<T>()` as the single-inspector shorthand, or `UseInspectors(...)` to
  compose an ordered, first-match-wins chain.
- **Deserializer** — turns the body into your message type. The default decodes the
  payload codec (UTF-8 JSON); override it per handler via the `Handle<,>` configuration
  with `WithDeserializer<T>()` (an `IMessageDeserializer`), or replace the deserialization
  stage for the whole topology with `UseDeserializationMiddleware<T>()`.
- **Middleware** — wraps the handler with cross-cutting stages (logging, metrics,
  transactions). Add your own `IMessageMiddleware` with
  `ConfigureInboundPipeline(pipeline => pipeline.UseMiddleware<T>())`; each stage decides
  whether and when to call the next.

```csharp
rabbit
    .ConfigureInboundPipeline(pipeline => pipeline.UseMiddleware<LoggingMiddleware>())
    .Consume(
        "orders-processing",
        consumer => consumer
            .UseInspector<LegacyHeaderInspector>()
            .Handle<OrderPlaced, OrderPlacedHandler>(handler => handler
                .WithDeserializer<XmlMessageDeserializer>()
                .ManualAck()
            )
    );
```

Composable inspector chains are useful when one queue carries several wire formats.
`CloudEvents()` adds the default CloudEvents inspector, `Use<TInspector>()` adds a
custom inspector resolved from DI, and recognizers map cheap transport signals to a
message type without writing an inspector class:

```csharp
rabbit.Consume(
    "uploads",
    consumer => consumer
        .UseInspectors(chain => chain
            .CloudEvents()
            .WhenHeader("x-amz-sns-message-type").As<UploadCompleted>("uploads.sns")
            .WhenContentType("application/vnd.legacy-upload+json").As<LegacyUpload>("uploads.legacy")
         )
        .Handle<UploadCompleted, UploadCompletedHandler>(handler => handler
            .WithDeserializer<SnsEnvelopeDeserializer>()
         )
        .Handle<LegacyUpload, LegacyUploadHandler>());
```

`WhenHeader(name)`, `WhenHeader(name, value)`, `WhenContentType(value)`, and
`When(Func<TransportMessage, bool>)` each finish with `As<T>()` or
`As<T>("explicit.discriminator")`. `As<T>()` uses the message contract registry;
the explicit overload is for inbound formats that are not CloudEvents contracts. Put
more specific entries first because the first match wins.

Recognizers only decide "what type is this?" The selected handler endpoint still owns
body decoding through `WithDeserializer<T>()`, so framed formats such as SNS envelopes
or non-JSON payloads should recognize in the chain and deserialize on the matching
handler.

### Acknowledgements

By default (`MessageAckMode.Auto`) Brilliant Messaging acknowledges a message once the handler
returns and negatively acknowledges it if the handler throws — the right behaviour for
most work. When you need the acknowledgement to hinge on something the handler does —
deferring it until downstream work has committed, for instance — switch a handler to
`MessageAckMode.Manual` and drive it yourself through the `IMessageAcknowledgement`
handle on `IncomingMessageContext`.

### Publisher confirms and mandatory routing

How sure do you need to be that a publish landed? That's the question
`RabbitMqPublisherConfirmMode` answers, configured per channel group:

- **`FireAndForget`** — publish and move on. Fastest, but a broker nack or an
  unroutable message disappears silently.
- **`Confirms`** — wait for the broker to confirm each publish, surfacing nacks and
  unroutable returns as a `MessageDeliveryException`.

Marking a target `Mandatory()` asks the broker to reject a message it can't route to
any queue. Because that rejection comes back asynchronously, Brilliant Messaging needs publisher
confirms to correlate it — so a mandatory target on a `FireAndForget` group is
rejected at build time with a `TopologyValidationException` rather than failing
mysteriously at runtime. Confirmation tracking serializes outstanding publishes per
channel (preserving order at one round-trip per publish); widen the channel group when
you'll trade strict ordering for throughput.

### Channel pooling and channel groups

Channels are not connections, but they aren't free either, and RabbitMQ frowns on
sharing one channel across threads. Brilliant Messaging manages a pool of channels over the topology's
single connection so your code never touches a raw `IChannel`. **Channel groups** are
how you tune that pool: each group has a maximum channel count and, on the outbound
side, its own publisher-confirm settings. Point a target at a named group with
`UseChannelGroup` to give a hot path its own dedicated, independently-tuned channels —
or just lean on the implicit defaults until a benchmark tells you otherwise.

Every channel counts against the broker's negotiated `channel_max`, so Brilliant Messaging sums its
worst-case channel budget across all groups and checks it against the limit the broker
advertises on the initial connection — an over-provisioned pool fails fast at startup
rather than starving for channels under load. Keep `channel_max` consistent across the
nodes of a RabbitMQ cluster so that check holds wherever a connection lands.

### Reliability and recovery

The RabbitMQ transport requires RabbitMQ.Client's automatic connection recovery
(`ConnectionFactory.AutomaticRecoveryEnabled = true`) and validates it at startup.
RabbitMQ.Client owns reconnection; Brilliant Messaging keeps using the same auto-recovering connection
for the topology's lifetime. Topology recovery (`TopologyRecoveryEnabled`, on by
default) restores exchanges, queues, and bindings — required for inbound topologies so
consumer subscriptions come back after a blip, and safe to disable when you provision
broker topology externally.

One honest caveat: automatic recovery is an *availability* mechanism, not a delivery
guarantee. It does not buffer or replay messages that were in flight during an outage.
If you need at-least-once effects, make your publishes safe to retry or put an outbox
in front of them.

### Observability

Brilliant Messaging instruments both hops of every message out of the box, using the BCL-native
`ActivitySource` and `Meter` primitives that OpenTelemetry consumes directly — there is
nothing to switch on in `BrilliantMessaging.Core`. A publish opens a `Producer` span and a delivery
opens a `Consumer` span parented to it across the broker, so a single trace follows a
message from publisher to handler. Spans and metrics are labeled with the
[OpenTelemetry `messaging.*` semantic conventions](https://opentelemetry.io/docs/specs/semconv/messaging/),
so Jaeger, Tempo, Grafana, Datadog, and Azure Monitor classify them as messaging
operations and light up their built-in messaging dashboards without any bespoke mapping.

Spans use `ActivityKind.Producer`/`ActivityKind.Consumer` and the convention span name
(`send {exchange}` / `process {queue}`), and carry `messaging.system`,
`messaging.operation.type`/`messaging.operation.name`, `messaging.destination.name`,
`messaging.rabbitmq.destination.routing_key` (when present), `messaging.message.id`, and
`messaging.message.body.size`. A failure adds a low-cardinality `error.type`; a
graceful-shutdown cancellation is deliberately *not* an error, so normal deploys don't
inflate your error-rate panels. The metric instruments are
`messaging.client.sent.messages`, `messaging.client.consumed.messages`, and
`messaging.client.operation.duration` (in seconds).

To collect any of this, point your `TracerProvider`/`MeterProvider` at Brilliant Messaging sources.
The `BrilliantMessaging.OpenTelemetry` package is the one-line way to do it:

```csharp
using BrilliantMessaging.OpenTelemetry;

services
    .AddOpenTelemetry()
    .WithTracing(tracing => tracing.AddBrilliantMessagingInstrumentation())
    .WithMetrics(metrics => metrics.AddBrilliantMessagingInstrumentation());
```

`AddBrilliantMessagingInstrumentation` registers the `BrilliantMessaging.Outbound` and `BrilliantMessaging.Inbound` activity sources
and meters. It references only `OpenTelemetry.Api`, so it forces no SDK choice on you,
and `BrilliantMessaging.Core` and the transports take no OpenTelemetry package reference at all. Without
the package you can subscribe to the same names directly with
`AddSource("BrilliantMessaging.Outbound", "BrilliantMessaging.Inbound")` and `AddMeter("BrilliantMessaging.Outbound", "BrilliantMessaging.Inbound")`;
the package is just the discoverable, named convenience.

### Builders and `IBuildable<T>`

Everything you configure — a topology, a consumer, an outbound target, an inspector
chain — is a fluent builder you receive inside a callback. Each builder ends in a single
terminal step that compiles your configuration into an immutable definition. That step is
the framework's job, not yours: you describe the topology, and Brilliant Messaging compiles it once when
the `Add…Topology` extension method returns. So the terminal `Build()` is deliberately
kept off the configuration surface — it isn't a public method on the builder and won't
show up in IntelliSense while you're configuring, which means you can't call it by
accident mid-callback.

The mechanism is a small interface, `IBuildable<T>`, that every builder implements
**explicitly**:

```csharp
public interface IBuildable<out TResult>
{
    TResult Build();
}
```

Because the implementation is explicit, `Build()` is reachable only through the interface,
never through the builder's own type. In normal use you never see it — the `Add…Topology`
extension methods on `IServiceCollection` configure the builder and compile it for you. If
you ever need to compile a builder yourself (building a `RabbitMqTopologyConfiguration`
directly in a test, say), cast to the interface to reach `Build()`:

```csharp
var topologyBuilder = new RabbitMqTopologyBuilder();
topologyBuilder.UseConnectionFactory(static _ => new ConnectionFactory());
topologyBuilder.Queue("inbound");
topologyBuilder.Consume("inbound", consumer => consumer.Handle<OrderPlaced, OrderPlacedHandler>());

var configuration = ((IBuildable<RabbitMqTopologyConfiguration>) topologyBuilder).Build();
```

The same pattern applies to every builder in the framework: the configuration surface is
public and discoverable, while the terminal `Build()` stays hidden in plain sight behind
`IBuildable<T>`.

## License

Brilliant Messaging is licensed under the [MIT License](LICENSE).

-----------------------------------------------------------------------------------------

<p align="center">
  <picture>
    <img alt="Brilliant Messaging Wallet" src="design/brilliantmessaging_wallet.png" width="900" />
  </picture>
</p>
