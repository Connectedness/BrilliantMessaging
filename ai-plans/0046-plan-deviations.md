# Plan Deviations — 0046 CloudEventEnvelope equality comparer

This file records how the implemented solution differs from the original plan in
`0046-cloudeventenvelope-equality-comparer.md`. It is not a plan, just a summary of the changes.

## Summary

The plan's correctness goal (symmetric, comparer-independent, ordinal extension equality consistent
with `GetHashCode`) was met, but the chosen mechanism is different and additionally removes a
per-comparison allocation that the plan would have kept.

## What changed relative to the plan

### Normalize once at construction instead of snapshotting on every comparison

- **Plan:** Make `ExtensionsEqual` comparer-independent by building a `Dictionary<string, string?>`
  keyed by `StringComparer.Ordinal` from `left` *inside each `Equals` call*, then iterating `right`
  against it.
- **Implemented:** The dictionary supplied to the constructor is normalized **once** into an
  ordinally keyed `Dictionary<string, string?>`, stored in a private `_extensions` backing field via
  `NormalizeToOrdinal`. `Equals`/`GetHashCode` then operate directly on that field, so neither
  allocates. `ExtensionsEqual` now takes two already-ordinal `Dictionary<string, string?>?`
  operands and does a direct `left.TryGetValue(...)`; the per-call snapshot is gone.

  Motivation: the plan's approach allocates a fresh `Dictionary` on every non-short-circuited
  comparison (relevant for hashing/dedup/`HashSet` scenarios). Moving normalization to construction
  makes the hot equality/hashing path allocation-free.

### Zero-copy fast path + lenient contract preserved (no validate-and-throw)

- The constructor parameter and the public `Extensions` property both stay typed as
  `IReadOnlyDictionary<string, string?>?`; the public API is unchanged and non-breaking.
- `NormalizeToOrdinal` returns the caller's dictionary **as-is** (no copy) when it is already a
  `Dictionary<string, string?>` whose comparer is ordinally behaving — `StringComparer.Ordinal` or
  `EqualityComparer<string>.Default` (see `IsOrdinalKeyComparer`). It copies into an ordinal
  dictionary only for the rare non-ordinal comparer (e.g., `OrdinalIgnoreCase`).
- We deliberately did **not** adopt a stricter "require a `Dictionary` with `StringComparer.Ordinal`
  and throw otherwise" design: a default `new Dictionary<string, string?> { ... }` exposes
  `EqualityComparer<string>.Default` (ordinal-behaving but not reference-equal to
  `StringComparer.Ordinal`), so a strict reference check would reject the most common construction
  and break existing callers/tests. Normalizing keeps the documented "compares regardless of the
  caller's comparer" behavior that the tests lock in.

### Record-struct backing field / property split

- `CloudEventEnvelope` remains a positional `readonly record struct`. The positional `Extensions`
  parameter is routed into the private `Dictionary<string, string?>? _extensions` field; a manually
  declared `Extensions` property exposes it as `IReadOnlyDictionary<string, string?>?`.
- The primary constructor writes `_extensions` directly (the field initializer is hoisted into the
  constructor), so normalization runs exactly once per construction — verified against the lowered
  IL. The property `init` accessor exists for object-initializer and `with { Extensions = ... }`
  usage and normalizes there too.

### `GetExtensionsHashCode` signature

- Left semantically intact (ordinal keys, order-independent XOR), but its parameter type was
  narrowed from `IReadOnlyDictionary<string, string?>?` to `Dictionary<string, string?>?` to match
  the stored field.

## Documentation refinement (commit 3d9857d, after the implementation)

The XML comments on `CloudEventEnvelope` were tightened to describe the actual behavior precisely:

- The `<remarks>` now state that extension keys are always matched ordinally for equality and
  hashing regardless of the supplied dictionary's comparer, that the envelope **may reuse** the
  supplied dictionary instance when it is already ordinally keyed (performance), and that callers
  should therefore treat `Extensions` as immutable.
- The `Extensions` property summary documents that the value is an ordinally keyed dictionary
  derived from the constructor argument (possibly the same instance).

  This last point is the one behavioral caveat worth flagging: because of the zero-copy fast path,
  the stored value can be the caller's own dictionary reference, so an envelope is only as immutable
  as the caller treats that dictionary — the same aliasing that existed before this change.

## Acceptance criteria

All original acceptance criteria remain satisfied: extension equality is symmetric and
comparer-independent, equality and `GetHashCode` are consistent (ordinal keys), tests cover
non-ordinal-comparer dictionaries, and the Release build produces no warnings.
