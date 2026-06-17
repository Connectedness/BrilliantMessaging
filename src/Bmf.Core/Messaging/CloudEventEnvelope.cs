using System;
using System.Collections.Generic;

namespace Bmf.Core.Messaging;

/// <summary>
/// Represents a transport-neutral CloudEvents v1.0 envelope in binary content mode.
/// </summary>
/// <remarks>
/// A transport binds these attributes according to its protocol binding. The RabbitMQ transport uses the AMQP
/// protocol binding over AMQP 0.9.1. Equality is structural with two deliberate refinements: <see cref="Data" />
/// is compared by content rather than by buffer identity, and <see cref="Extensions" /> is compared as an
/// unordered set of key/value pairs.
/// </remarks>
/// <param name="SpecVersion">The CloudEvents specification version.</param>
/// <param name="Id">The event identifier.</param>
/// <param name="Source">The event source.</param>
/// <param name="Type">The event type discriminator.</param>
/// <param name="Time">The time at which the event occurred.</param>
/// <param name="Subject">The optional subject of the event, or <see langword="null" />.</param>
/// <param name="DataContentType">The content type of <paramref name="Data" />.</param>
/// <param name="DataSchema">The optional schema of <paramref name="Data" />, or <see langword="null" />.</param>
/// <param name="Data">The serialized event data.</param>
/// <param name="Extensions">The optional CloudEvents extension attributes, or <see langword="null" />.</param>
public readonly record struct CloudEventEnvelope(
    string SpecVersion,
    string Id,
    string Source,
    string Type,
    DateTimeOffset Time,
    string? Subject,
    string DataContentType,
    string? DataSchema,
    ReadOnlyMemory<byte> Data,
    IReadOnlyDictionary<string, string?>? Extensions = null
)
{
    /// <summary>
    /// Determines whether this envelope is structurally equal to another, comparing <see cref="Data" /> by
    /// content and <see cref="Extensions" /> as an unordered set of key/value pairs.
    /// </summary>
    /// <param name="other">The envelope to compare with.</param>
    /// <returns><see langword="true" /> when the envelopes are equal; otherwise <see langword="false" />.</returns>
    public bool Equals(CloudEventEnvelope other)
    {
        return SpecVersion == other.SpecVersion &&
               Id == other.Id &&
               Source == other.Source &&
               Type == other.Type &&
               Time.Equals(other.Time) &&
               Subject == other.Subject &&
               DataContentType == other.DataContentType &&
               DataSchema == other.DataSchema &&
               ReadOnlyMemoryByteEqualityComparer.Default.Equals(Data, other.Data) &&
               ExtensionsEqual(Extensions, other.Extensions);
    }

    /// <summary>
    /// Returns a hash code consistent with <see cref="Equals(CloudEventEnvelope)" />: <see cref="Data" />
    /// contributes a content-based hash and <see cref="Extensions" /> an order-independent one.
    /// </summary>
    /// <returns>The hash code for this envelope.</returns>
    public override int GetHashCode()
    {
        var hashCode = new HashCode();
        hashCode.Add(SpecVersion);
        hashCode.Add(Id);
        hashCode.Add(Source);
        hashCode.Add(Type);
        hashCode.Add(Time);
        hashCode.Add(Subject);
        hashCode.Add(DataContentType);
        hashCode.Add(DataSchema);
        hashCode.Add(ReadOnlyMemoryByteEqualityComparer.Default.GetHashCode(Data));
        hashCode.Add(GetExtensionsHashCode(Extensions));
        return hashCode.ToHashCode();
    }

    private static bool ExtensionsEqual(
        IReadOnlyDictionary<string, string?>? left,
        IReadOnlyDictionary<string, string?>? right
    )
    {
        if (ReferenceEquals(left, right))
        {
            return true;
        }

        if (left is null || right is null || left.Count != right.Count)
        {
            return false;
        }

        foreach (var pair in left)
        {
            if (!right.TryGetValue(pair.Key, out var value) ||
                !string.Equals(pair.Value, value, StringComparison.Ordinal))
            {
                return false;
            }
        }

        return true;
    }

    private static int GetExtensionsHashCode(IReadOnlyDictionary<string, string?>? extensions)
    {
        if (extensions is null)
        {
            return 0;
        }

        // XOR keeps the hash independent of enumeration order, mirroring the unordered equality above.
        var hashCode = 0;
        foreach (var pair in extensions)
        {
            hashCode ^= HashCode.Combine(pair.Key, pair.Value);
        }

        return hashCode;
    }
}
