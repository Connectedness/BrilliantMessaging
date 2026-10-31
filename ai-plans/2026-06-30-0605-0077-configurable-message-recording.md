# Make in-memory transport message recording configurable

> Issue: [#77](https://github.com/Connectedness/BrilliantMessaging/issues/77)

## Rationale

The in-memory transport (added in #0075) records **every** message routed to **every** topic for
the lifetime of the owning service provider, backing the `InMemoryBroker.GetMessages(string topic)`
inspection API. Recording everything is the right default for the primary use case — short-lived
providers in unit/integration tests — but there is a second, legitimate use case: running the
in-memory transport as a long-lived broker substitute in a test environment or local-development
host. There the recording accumulates indefinitely (each `InMemoryTransportMessage` retains the
serialized body and headers) and grows into a memory problem with no way to bound or disable it.

Keep record-all as the default, but make it configurable through a single topology-level setting with
three modes — off, unbounded (default), and bounded — plus a clear/reset method on `InMemoryBroker`.
The bounded mode keeps the last N recorded messages per topic and evicts the oldest when the cap is
reached, serving the long-lived-host case without unbounded growth. It is strictly opt-in: an unbounded
or absent setting preserves the "`GetMessages` returns everything routed" contract that tests rely on
for exact-count assertions, and the truncation behavior of the bounded mode is explicitly documented.

## Acceptance Criteria

- [x] Recording is controlled at the topology level by a single `RecordMessages` setting with three modes: off (`RecordMessages(false)`), unbounded (`RecordMessages()`, the default), and bounded (`RecordMessages(maxPerTopic: N)`).
- [x] When recording is disabled, `GetMessages` returns an empty list and the internal recordings dictionary stays empty (the topic key is never created).
- [x] In bounded mode, each topic retains at most N recorded messages; once the cap is reached the oldest recorded message is evicted as newer ones arrive.
- [x] The default (and the parameterless `RecordMessages()`) remains unbounded record-all; bounded truncation never happens unless explicitly opted in.
- [x] `maxPerTopic` is validated (must be positive); an invalid value throws `ArgumentOutOfRangeException`.
- [x] `InMemoryBroker` exposes a way to clear recorded messages, both for all topics and for a single topic.
- [x] New public APIs are XML-documented and follow the repository's explicit `using` convention.
- [x] `docs/in-memory-transport.md` documents the three `RecordMessages` modes (off / unbounded / bounded), the bounded-mode truncation caveat, and the `ClearRecordings` API; the existing memory-growth caveat is updated to reference disabling or bounding recording.
- [x] Automated tests cover the disabled-recording and clear paths.
- [x] Automated tests cover bounded-mode eviction: a topic capped at N retains exactly N messages after more than N are routed, dropping the oldest in routing order (asserted after `DrainUntilIdleAsync` so trimming has settled).
- [x] Automated tests confirm the recording setting governs the dead-letter republish path, not just normal routing: with recording off, `GetMessages(deadLetterTopic)` stays empty after a message is dead-lettered (and, optionally, a bounded dead-letter topic is capped the same way).
- [x] Release builds stay warning-clean with `TreatWarningsAsErrors`.

## Technical Details

### Topology-level recording setting

Model recording as a single value with three modes rather than independent flags, so the modes cannot
contradict each other. Introduce a small public `readonly record struct InMemoryRecordingOptions`
(carrying `Enabled` and a nullable `MaxPerTopic`, where `MaxPerTopic == null` means unbounded) that
threads through the configuration pipeline, mirroring how `ShutdownTimeout` already flows. Make the
illegal states unrepresentable: give the struct a **private constructor** and expose the three modes
only through static factories — `InMemoryRecordingOptions.Off`, `InMemoryRecordingOptions.Unbounded`,
and `InMemoryRecordingOptions.Bounded(int maxPerTopic)` — with the `maxPerTopic > 0` validation
centralized in `Bounded(...)` (throwing `ArgumentOutOfRangeException`). This prevents callers from
constructing a contradictory `Off`-but-bounded value or bypassing validation, and the builder
overloads below become thin adapters over these factories. Note that, because this is a struct, its
zero value (`default(InMemoryRecordingOptions)`) is `Enabled = false` — i.e. `Off`, not the documented
`Unbounded` default — so the builder must always supply the recording value explicitly (its field
defaults to `InMemoryRecordingOptions.Unbounded`) and `default` must not be relied on as a stand-in
for the default mode:

- **`InMemoryTopologyBuilder`** — hold the recording setting (default: enabled, unbounded) and expose
  overloads:
    - `RecordMessages()` — enabled, unbounded (the explicit form of the default); maps to
      `InMemoryRecordingOptions.Unbounded`.
    - `RecordMessages(bool record)` — `false` disables recording entirely (`Off`); `true` enables
      unbounded recording (`Unbounded`). Document in the XML doc that `true` is equivalent to the
      parameterless `RecordMessages()`.
    - `RecordMessages(int maxPerTopic)` — enabled, bounded; maps to `InMemoryRecordingOptions.Bounded`,
      which validates `maxPerTopic > 0` and throws `ArgumentOutOfRangeException` otherwise; name the
      parameter `maxPerTopic` for call-site clarity, matching the issue's preferred shape.

  Each overload returns the builder for chaining. Surface the same overloads on both
  `IInMemoryInboundTopologyBuilder` and `IInMemoryOutboundTopologyBuilder` (explicit interface
  implementations delegating to the concrete methods, as existing members do). Pass the setting into
  the constructed `InMemoryTopologyConfiguration` in `IBuildable.Build()`.
- **`InMemoryTopologyConfiguration`** — add a record parameter carrying the recording setting and
  document it.
- **`InMemoryTopologyCompiler.Compile`** — pass the recording setting into the `InMemoryBroker`
  constructor.
- **`InMemoryBroker`** — accept the setting and apply it in `Record(...)` (called from `RouteCore`,
  including the dead-letter republish path):
    - **Off** — skip recording; the `_recordings` dictionary stays empty and `GetMessages` returns `[]`,
      so no change is needed there.
    - **Unbounded** — current behavior.
    - **Bounded(N)** — after enqueuing, trim the topic's `ConcurrentQueue` back to N by dequeuing the
      oldest (`while (queue.Count > N) queue.TryDequeue(out _)`). Note that under concurrent routing the
      queue length may momentarily exceed N before trimming settles, so the cap is an upper bound rather
      than an exact-at-every-instant invariant; document this. (If a stricter guarantee is wanted, a
      per-topic lock around enqueue+trim is the fallback, at the cost of contention.) Be aware that
      `ConcurrentQueue.Count` is a snapshot that can walk segments, so bounded mode adds a small
      per-recorded-message cost; this is acceptable for the long-lived-host use case the mode targets,
      and the simple loop is preferred over maintaining a separate `Interlocked` length counter.

### Clear/reset on `InMemoryBroker`

Add two public overloads — `ClearRecordings()` (all topics) and `ClearRecordings(string topic)`
(single topic), consistent with `GetMessages(string topic)`. Implementation operates on the existing
`ConcurrentDictionary<string, ConcurrentQueue<InMemoryTransportMessage>> _recordings`: clear-all
calls `_recordings.Clear()`; single-topic calls `_recordings.TryRemove(topic, out _)` so the
post-clear state is identical to a topic that was never recorded (key absent, `GetMessages` returns
`[]`) — this is the race-friendlier option and indistinguishable from clear-in-place from the
caller's perspective. Clearing a topic that was never recorded is a no-op. Validate the topic
argument the same way `GetMessages` does
(throw `ArgumentException` on null/whitespace). This is independent of the toggle and changes no
defaults.

### Docs

Update the Support API section of `docs/in-memory-transport.md`: document the three `RecordMessages`
modes (off, unbounded default, bounded `maxPerTopic`) and the `ClearRecordings` API. Rewrite the
memory-growth caveat to point at disabling or bounding recording as the remedy for long-running hosts,
and call out explicitly that bounded mode truncates — `GetMessages` then returns only the most recent N
per topic and exact-count assertions across more than N routed messages will not hold, so tests that
rely on full history should keep the default unbounded mode.
