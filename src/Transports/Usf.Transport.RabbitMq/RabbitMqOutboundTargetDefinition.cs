using System;
using System.Collections.Generic;

namespace Usf.Transport.RabbitMq;

public abstract record RabbitMqOutboundTargetDefinition(
    Type MessageType,
    string AddressName,
    string? ChannelGroupName,
    string? TargetName,
    Type? SerializerType,
    bool IsMandatory
);

public sealed record RabbitMqFanoutOutboundTargetDefinition(
    Type MessageType,
    string AddressName,
    string? ChannelGroupName,
    string? TargetName,
    Type? SerializerType,
    bool IsMandatory
) : RabbitMqOutboundTargetDefinition(
    MessageType,
    AddressName,
    ChannelGroupName,
    TargetName,
    SerializerType,
    IsMandatory
);

public abstract record RabbitMqRoutingKeyOutboundTargetDefinition(
    Type MessageType,
    string AddressName,
    string? ChannelGroupName,
    string? TargetName,
    Type? SerializerType,
    bool IsMandatory,
    string? RoutingKey,
    Delegate? RoutingKeyFactory
) : RabbitMqOutboundTargetDefinition(
    MessageType,
    AddressName,
    ChannelGroupName,
    TargetName,
    SerializerType,
    IsMandatory
);

public sealed record RabbitMqDirectOutboundTargetDefinition(
    Type MessageType,
    string AddressName,
    string? ChannelGroupName,
    string? TargetName,
    Type? SerializerType,
    bool IsMandatory,
    string? RoutingKey,
    Delegate? RoutingKeyFactory
) : RabbitMqRoutingKeyOutboundTargetDefinition(
    MessageType,
    AddressName,
    ChannelGroupName,
    TargetName,
    SerializerType,
    IsMandatory,
    RoutingKey,
    RoutingKeyFactory
);

public sealed record RabbitMqTopicOutboundTargetDefinition(
    Type MessageType,
    string AddressName,
    string? ChannelGroupName,
    string? TargetName,
    Type? SerializerType,
    bool IsMandatory,
    string? RoutingKey,
    Delegate? RoutingKeyFactory
) : RabbitMqRoutingKeyOutboundTargetDefinition(
    MessageType,
    AddressName,
    ChannelGroupName,
    TargetName,
    SerializerType,
    IsMandatory,
    RoutingKey,
    RoutingKeyFactory
);

public sealed record RabbitMqHeadersOutboundTargetDefinition(
    Type MessageType,
    string AddressName,
    string? ChannelGroupName,
    string? TargetName,
    Type? SerializerType,
    bool IsMandatory,
    IReadOnlyDictionary<string, object?> Headers
) : RabbitMqOutboundTargetDefinition(
    MessageType,
    AddressName,
    ChannelGroupName,
    TargetName,
    SerializerType,
    IsMandatory
);
