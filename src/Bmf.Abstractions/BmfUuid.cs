using System;

namespace BrilliantMessaging.Abstractions;

/// <summary>
/// Creates time-ordered identifiers for BrilliantMessaging messages.
/// </summary>
public static class BrilliantMessagingUuid
{
    /// <summary>
    /// Creates a ULID projected to a <see cref="Guid" />.
    /// </summary>
    /// <remarks>
    /// The result is time ordered like a UUIDv7, but it is not bit-for-bit RFC 9562 UUIDv7. CloudEvents treats
    /// its wire-level id as an opaque string. This helper centralizes generation so the implementation can be
    /// replaced without changing message contracts.
    /// </remarks>
    public static Guid NewId()
    {
        return Ulid.NewUlid().ToGuid();
    }
}
