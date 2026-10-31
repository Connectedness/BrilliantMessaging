# CloudEventEnvelope equality relies on caller's dictionary key comparer

## Rationale

`CloudEventEnvelope` exposes a public, documented structural-equality contract, but its
`Extensions` comparison is unsound when a caller supplies an `IReadOnlyDictionary` with a
non-ordinal key comparer (e.g. `StringComparer.OrdinalIgnoreCase`):

1. **Asymmetric equality** — `ExtensionsEqual` enumerates `left` but looks keys up via
   `right.TryGetValue(pair.Key, ...)`, deferring to the *right* dictionary's comparer. When the
   two dictionaries use different comparers, `a.Equals(b)` can differ from `b.Equals(a)`,
   violating the symmetry requirement of `Equals`.
2. **Equals/GetHashCode mismatch** — `GetExtensionsHashCode` hashes keys ordinally via
   `HashCode.Combine(pair.Key, pair.Value)`. Keys that compare equal under a case-insensitive
   comparer hash differently, so equal envelopes can produce different hash codes.

Practical likelihood is low (CloudEvents extension names are spec-restricted to lowercase
`a-z0-9`, so callers normally pass ordinal dictionaries), but the contract should be solid and
comparer-independent before release.

## Acceptance Criteria

- [x] `CloudEventEnvelope` extension equality is symmetric regardless of the key comparer used by
      either caller-supplied dictionary.
- [x] Extension equality and `GetExtensionsHashCode` are consistent: keys are matched and hashed
      ordinally, so two envelopes that are `Equals` always yield the same hash code.
- [x] Automated tests need to be written, including cases where extension dictionaries are built
      with a non-ordinal comparer (e.g. `OrdinalIgnoreCase`) to lock in symmetric, ordinal
      behaviour.
- [x] No new build warnings (Release builds treat warnings as errors).

## Technical Details

In `src/Bmf.Core/Messaging/CloudEventEnvelope.cs`:

- Make `ExtensionsEqual` comparer-independent by matching keys ordinally instead of relying on
  `right.TryGetValue`. Snapshot one side into an explicitly `StringComparer.Ordinal`-keyed lookup
  (e.g. a `Dictionary<string, string?>(StringComparer.Ordinal)` built from `left`) and iterate the
  other side against it, comparing values with `StringComparison.Ordinal`. Keep the existing
  `ReferenceEquals`, null, and `Count` short-circuits.
- `GetExtensionsHashCode` already hashes keys ordinally and is order-independent via XOR; leave its
  approach intact so it stays consistent with the now-ordinal equality.
