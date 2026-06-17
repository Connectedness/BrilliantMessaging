using System;

namespace Bmf.Core.Messaging.Inbound;

/// <summary>
/// The outcome of inspecting an inbound transport message: the resolved CloudEvents discriminator and the
/// concrete message type, optionally with an already-materialized message and pre-seeded context items.
/// </summary>
/// <param name="Discriminator">The CloudEvents <c>type</c> discriminator the message was published under.</param>
/// <param name="MessageType">The concrete message type the body should be deserialized into.</param>
public sealed record InboundMessageInspectionResult(string Discriminator, Type MessageType)
{
    /// <summary>
    /// Gets the message instance when the inspector already materialized it; otherwise <see langword="null" />,
    /// in which case the deserialization middleware decodes the body.
    /// </summary>
    public object? Message { get; init; }

    /// <summary>
    /// Gets the context items the inspector contributes for the message, or <see langword="null" /> when it
    /// contributes none.
    /// </summary>
    public IncomingMessageContextItems? Items { get; init; }
}
