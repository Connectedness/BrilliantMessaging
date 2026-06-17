using System;
using System.Collections.Generic;

namespace Bmf.Core.Messaging;

/// <summary>
/// A single message-contract mapping accumulated by <see cref="MessageContractRegistryBuilder" /> before the
/// immutable <see cref="MessageContractRegistry" /> is built.
/// </summary>
public sealed class MessageContractRegistration
{
    /// <summary>
    /// Initializes a new instance of the <see cref="MessageContractRegistration" /> class.
    /// </summary>
    /// <param name="messageType">The message type being registered.</param>
    /// <param name="discriminator">The canonical CloudEvents <c>type</c> discriminator.</param>
    /// <param name="acceptsCanonicalInbound">Whether the canonical discriminator is also accepted inbound.</param>
    public MessageContractRegistration(Type messageType, string discriminator, bool acceptsCanonicalInbound)
    {
        MessageType = messageType;
        Discriminator = discriminator;
        AcceptsCanonicalInbound = acceptsCanonicalInbound;
    }

    /// <summary>
    /// Gets a value indicating whether the canonical discriminator is accepted inbound (true for
    /// <see cref="MessageContractRegistryBuilder.Map{T}" />, false for <see cref="MessageContractRegistryBuilder.MapOutbound{T}" />).
    /// </summary>
    public bool AcceptsCanonicalInbound { get; }

    /// <summary>
    /// Gets or sets the CloudEvents <c>dataschema</c> attached to messages of this type, or <see langword="null" /> for none.
    /// </summary>
    public string? DataSchema { get; set; }

    /// <summary>
    /// Gets the canonical CloudEvents <c>type</c> discriminator for the message type.
    /// </summary>
    public string Discriminator { get; }

    /// <summary>
    /// Gets the additional inbound discriminators (aliases) accepted for the message type.
    /// </summary>
    public List<string> InboundAliases { get; } = [];

    /// <summary>
    /// Gets the message type being registered.
    /// </summary>
    public Type MessageType { get; }
}
