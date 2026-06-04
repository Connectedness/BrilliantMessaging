using System;
using System.Collections.Generic;

namespace Usf.Core.Messaging;

public sealed class MessageContractRegistration
{
    public MessageContractRegistration(Type messageType, string discriminator, bool acceptsCanonicalInbound)
    {
        MessageType = messageType;
        Discriminator = discriminator;
        AcceptsCanonicalInbound = acceptsCanonicalInbound;
    }

    public bool AcceptsCanonicalInbound { get; }

    public string? DataSchema { get; set; }

    public string Discriminator { get; }

    public List<string> InboundAliases { get; } = [];

    public Type MessageType { get; }
}
