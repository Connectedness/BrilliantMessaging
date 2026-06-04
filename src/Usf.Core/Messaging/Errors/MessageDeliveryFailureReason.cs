namespace Usf.Core.Messaging.Errors;

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
    Nacked = 0,
    Returned = 1,
    Timeout = 2
}
