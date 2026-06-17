using System;

namespace Bmf.Transport.RabbitMq.Outbound;

/// <summary>
/// The immutable base for routing-key-based outbound target declarations (direct and topic exchanges), adding a
/// fixed routing key or a per-message routing-key factory.
/// </summary>
/// <param name="MessageType">The message type the target publishes.</param>
/// <param name="ExchangeName">The name of the exchange messages are published to.</param>
/// <param name="ChannelGroupName">The channel group to publish through, or <see langword="null" /> for the default group.</param>
/// <param name="TargetName">The explicit target name, or <see langword="null" /> to derive one.</param>
/// <param name="SerializerType">The serializer type override, or <see langword="null" /> for the framework default.</param>
/// <param name="IsMandatory">Whether the target requests mandatory routing.</param>
/// <param name="RoutingKey">The fixed routing key, or <see langword="null" /> when a factory is used.</param>
/// <param name="RoutingKeyFactory">The per-message routing-key factory, or <see langword="null" /> when a fixed key is used.</param>
public abstract record RabbitMqRoutingKeyOutboundTargetDefinition(
    Type MessageType,
    string ExchangeName,
    string? ChannelGroupName,
    string? TargetName,
    Type? SerializerType,
    bool IsMandatory,
    string? RoutingKey,
    Delegate? RoutingKeyFactory
) : RabbitMqOutboundTargetDefinition(
    MessageType,
    ExchangeName,
    ChannelGroupName,
    TargetName,
    SerializerType,
    IsMandatory
);
