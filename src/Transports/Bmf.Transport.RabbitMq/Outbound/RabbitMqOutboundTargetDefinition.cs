using System;

namespace Bmf.Transport.RabbitMq.Outbound;

/// <summary>
/// The immutable base for a RabbitMQ outbound target declaration produced by
/// <see cref="RabbitMqOutboundTargetBuilder{TMessage}" />. Concrete subtypes add the routing detail for each
/// exchange type.
/// </summary>
/// <param name="MessageType">The message type the target publishes.</param>
/// <param name="ExchangeName">The name of the exchange messages are published to.</param>
/// <param name="ChannelGroupName">The channel group to publish through, or <see langword="null" /> for the default group.</param>
/// <param name="TargetName">The explicit target name, or <see langword="null" /> to derive one.</param>
/// <param name="SerializerType">The serializer type override, or <see langword="null" /> for the framework default.</param>
/// <param name="IsMandatory">Whether the target requests mandatory routing.</param>
public abstract record RabbitMqOutboundTargetDefinition(
    Type MessageType,
    string ExchangeName,
    string? ChannelGroupName,
    string? TargetName,
    Type? SerializerType,
    bool IsMandatory
);
