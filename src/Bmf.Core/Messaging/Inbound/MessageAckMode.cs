namespace Bmf.Core.Messaging.Inbound;

/// <summary>
/// Controls when an inbound message is acknowledged to the transport.
/// </summary>
/// <remarks>
/// Choose <see cref="Auto" /> for the common case where the framework should acknowledge a message once the
/// handler completes successfully and negatively acknowledge it when the handler throws. Choose
/// <see cref="Manual" /> when the handler needs to control acknowledgement itself — for example to defer the
/// acknowledgement until downstream work has been committed.
/// </remarks>
public enum MessageAckMode
{
    /// <summary>
    /// The framework acknowledges the message after the handler succeeds and negatively acknowledges it when the
    /// handler throws.
    /// </summary>
    Auto = 0,

    /// <summary>
    /// The handler is responsible for acknowledging the message through <see cref="IMessageAcknowledgement" />.
    /// </summary>
    Manual = 1
}
