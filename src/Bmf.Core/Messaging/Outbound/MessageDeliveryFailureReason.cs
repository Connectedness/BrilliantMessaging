namespace Bmf.Core.Messaging.Outbound;

/// <summary>
/// Identifies why a message could not be delivered, reported through <see cref="MessageDeliveryException" />.
/// </summary>
/// <remarks>
/// This vocabulary is intentionally small and broker-neutral so it can be reused across transports: a publish
/// is either rejected by the broker (<see cref="Nacked" />), accepted but routed nowhere
/// (<see cref="Returned" />), or never confirmed within the bounded wait (<see cref="Timeout" />). Resist
/// padding it with broker-specific reasons; transport-specific detail belongs on the originating exception
/// carried as <see cref="System.Exception.InnerException" />, not in this enum.
/// </remarks>
public enum MessageDeliveryFailureReason
{
    /// <summary>
    /// The broker rejected the publish with a negative acknowledgement (nack).
    /// </summary>
    Nacked = 0,

    /// <summary>
    /// The broker accepted the publish but could not route it to any queue and returned it.
    /// </summary>
    Returned = 1,

    /// <summary>
    /// The publish was not confirmed by the broker within the bounded wait.
    /// </summary>
    Timeout = 2
}
