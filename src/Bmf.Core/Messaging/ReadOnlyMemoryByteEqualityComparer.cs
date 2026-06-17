using System;
using System.Collections.Generic;

namespace Bmf.Core.Messaging;

/// <summary>
/// Compares <see cref="ReadOnlyMemory{T}" /> of <see cref="byte" /> by content rather than by reference, so two
/// distinct buffers with the same bytes are considered equal. Used to give <see cref="CloudEventEnvelope" /> its
/// content-based equality.
/// </summary>
public sealed class ReadOnlyMemoryByteEqualityComparer : IEqualityComparer<ReadOnlyMemory<byte>>
{
    private ReadOnlyMemoryByteEqualityComparer() { }

    /// <summary>
    /// Gets the shared comparer instance.
    /// </summary>
    public static ReadOnlyMemoryByteEqualityComparer Default { get; } = new ();

    /// <inheritdoc />
    public bool Equals(ReadOnlyMemory<byte> x, ReadOnlyMemory<byte> y)
    {
        return x.Span.SequenceEqual(y.Span);
    }

    /// <inheritdoc />
    public int GetHashCode(ReadOnlyMemory<byte> value)
    {
        // We calculate the hash code with only the length and the last byte. This lets us achieve O(1) performance
        // instead of O(n) for longer spans.
        var span = value.Span;
        return span.IsEmpty ? 0 : HashCode.Combine(span.Length, span[^1]);
    }
}
