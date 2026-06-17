using System;
using System.Collections.Generic;

namespace Bmf.Transport.RabbitMq.Outbound;

/// <summary>
/// An immutable outbound target declaration for a headers exchange (routes on the configured headers).
/// </summary>
/// <param name="MessageType">The message type the target publishes.</param>
/// <param name="ExchangeName">The name of the headers exchange.</param>
/// <param name="ChannelGroupName">The channel group to publish through, or <see langword="null" /> for the default group.</param>
/// <param name="TargetName">The explicit target name, or <see langword="null" /> to derive one.</param>
/// <param name="SerializerType">The serializer type override, or <see langword="null" /> for the framework default.</param>
/// <param name="IsMandatory">Whether the target requests mandatory routing.</param>
/// <param name="Headers">The headers attached to every published message and used for routing.</param>
public sealed record RabbitMqHeadersOutboundTargetDefinition(
    Type MessageType,
    string ExchangeName,
    string? ChannelGroupName,
    string? TargetName,
    Type? SerializerType,
    bool IsMandatory,
    IReadOnlyDictionary<string, object?> Headers
) : RabbitMqOutboundTargetDefinition(
    MessageType,
    ExchangeName,
    ChannelGroupName,
    TargetName,
    SerializerType,
    IsMandatory
);
