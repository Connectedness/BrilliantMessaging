using System;

namespace Bmf.Core.Messaging.Inbound;

/// <summary>
/// Thrown when an inbound message carries a CloudEvents discriminator that is not registered, so no message
/// type can be resolved for it.
/// </summary>
public sealed class UnknownInboundMessageException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="UnknownInboundMessageException" /> class.
    /// </summary>
    /// <param name="source">The transport source the message arrived from.</param>
    /// <param name="discriminator">The unregistered CloudEvents discriminator.</param>
    /// <param name="message">An optional explicit message; when omitted a default is composed.</param>
    public UnknownInboundMessageException(string source, string discriminator, string? message = null)
        : base(message ?? $"Inbound message discriminator '{discriminator}' from '{source}' is not registered.")
    {
        TransportSource = source;
        Discriminator = discriminator;
    }

    /// <summary>
    /// Gets the transport source the message arrived from.
    /// </summary>
    public string TransportSource { get; }

    /// <summary>
    /// Gets the unregistered CloudEvents discriminator.
    /// </summary>
    public string Discriminator { get; }
}
