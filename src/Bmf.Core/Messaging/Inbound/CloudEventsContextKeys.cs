namespace Bmf.Core.Messaging.Inbound;

/// <summary>
/// The well-known <see cref="MessageContextKey{T}" /> keys under which CloudEvents data is stored in the inbound
/// message context.
/// </summary>
public static class CloudEventsContextKeys
{
    /// <summary>
    /// The key under which the reconstructed <see cref="CloudEventEnvelope" /> is stored for the current message.
    /// </summary>
    public static MessageContextKey<CloudEventEnvelope> Envelope { get; } = new ("cloudevents.envelope");
}
