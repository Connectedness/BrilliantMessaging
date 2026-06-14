using System;

namespace Usf.Core.Messaging;

public sealed record InboundMessageInspectionResult(string Discriminator, Type MessageType)
{
    public object? Message { get; init; }

    public IncomingMessageContextItems? Items { get; init; }
}
