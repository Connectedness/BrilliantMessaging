using System;
using System.Collections.Generic;

namespace BrilliantMessaging.Core.Messaging;

/// <summary>
/// The trace parent, trace state, and baggage extracted from inbound transport headers.
/// </summary>
/// <param name="TraceParent">The extracted W3C <c>traceparent</c> value, or <see langword="null" /> when absent.</param>
/// <param name="TraceState">The extracted W3C <c>tracestate</c> value, or <see langword="null" /> when absent.</param>
/// <param name="Baggage">
/// The extracted baggage as a read-only map of key to value. It is exposed read-only so callers cannot mutate the
/// extracted value; a caller that downcasts to the backing <see cref="Dictionary{TKey,TValue}" /> takes that
/// responsibility on deliberately.
/// </param>
public sealed record TraceContextHeadersExtractResult(
    string? TraceParent,
    string? TraceState,
    IReadOnlyDictionary<string, string?> Baggage
)
{
    /// <summary>
    /// Determines value equality: two results are equal when their <see cref="TraceParent" /> and
    /// <see cref="TraceState" /> match ordinally and their <see cref="Baggage" /> describes the same set of
    /// key/value pairs. The synthesized member equality a positional record provides would instead compare
    /// <see cref="Baggage" /> by reference, so it is replaced here to give the type proper value-object semantics.
    /// </summary>
    /// <param name="other">The result to compare with.</param>
    /// <returns><see langword="true" /> when the results are equal; otherwise <see langword="false" />.</returns>
    public bool Equals(TraceContextHeadersExtractResult? other)
    {
        if (other is null)
        {
            return false;
        }

        if (ReferenceEquals(this, other))
        {
            return true;
        }

        return string.Equals(TraceParent, other.TraceParent, StringComparison.Ordinal) &&
               string.Equals(TraceState, other.TraceState, StringComparison.Ordinal) &&
               BaggageEqual(Baggage, other.Baggage);
    }

    /// <summary>
    /// Returns a hash code consistent with <see cref="Equals(TraceContextHeadersExtractResult)" />:
    /// <see cref="Baggage" /> contributes an order-independent hash so that results carrying the same key/value
    /// pairs hash alike regardless of enumeration order.
    /// </summary>
    /// <returns>The hash code for this result.</returns>
    public override int GetHashCode()
    {
        var hashCode = new HashCode();
        hashCode.Add(TraceParent);
        hashCode.Add(TraceState);
        hashCode.Add(GetBaggageHashCode(Baggage));
        return hashCode.ToHashCode();
    }

    private static bool BaggageEqual(
        IReadOnlyDictionary<string, string?> left,
        IReadOnlyDictionary<string, string?> right
    )
    {
        if (ReferenceEquals(left, right))
        {
            return true;
        }

        if (left.Count != right.Count)
        {
            return false;
        }

        // Enumerate via the concrete dictionary's struct enumerator so equality stays allocation-free; iterating
        // through the IReadOnlyDictionary interface would box the enumerator. Extract always supplies a Dictionary;
        // a caller-supplied read-only dictionary of another kind takes the (allocating) interface fallback.
        if (left is Dictionary<string, string?> leftDictionary)
        {
            foreach (var pair in leftDictionary)
            {
                if (!ContainsEqualValue(right, pair))
                {
                    return false;
                }
            }

            return true;
        }

        foreach (var pair in left)
        {
            if (!ContainsEqualValue(right, pair))
            {
                return false;
            }
        }

        return true;

        static bool ContainsEqualValue(
            IReadOnlyDictionary<string, string?> dictionary,
            KeyValuePair<string, string?> pair
        )
        {
            return dictionary.TryGetValue(pair.Key, out var value) &&
                   string.Equals(pair.Value, value, StringComparison.Ordinal);
        }
    }

    private static int GetBaggageHashCode(IReadOnlyDictionary<string, string?> baggage)
    {
        // XOR keeps the baggage hash independent of enumeration order, mirroring the unordered equality above. As
        // in BaggageEqual, the concrete dictionary is enumerated through its struct enumerator to avoid allocating.
        var hashCode = 0;

        if (baggage is Dictionary<string, string?> dictionary)
        {
            foreach (var pair in dictionary)
            {
                hashCode ^= HashCode.Combine(pair.Key, pair.Value);
            }

            return hashCode;
        }

        foreach (var pair in baggage)
        {
            hashCode ^= HashCode.Combine(pair.Key, pair.Value);
        }

        return hashCode;
    }
}
