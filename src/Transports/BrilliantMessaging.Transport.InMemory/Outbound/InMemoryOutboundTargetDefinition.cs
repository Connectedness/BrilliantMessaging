using System;

namespace BrilliantMessaging.Transport.InMemory.Outbound;

/// <summary>
/// An immutable declaration of an in-memory outbound target, produced by
/// <see cref="InMemoryOutboundTargetBuilder{TMessage}" />. It maps a message type to a declared topic.
/// </summary>
/// <param name="MessageType">The message type the target publishes.</param>
/// <param name="Topic">The declared topic published messages are routed to.</param>
/// <param name="TargetName">The optional explicit target name, or <see langword="null" /> for the default target of the message type.</param>
/// <param name="SerializerType">The serializer type, or <see langword="null" /> for the framework default.</param>
public sealed record InMemoryOutboundTargetDefinition(
    Type MessageType,
    string Topic,
    string? TargetName,
    Type? SerializerType
);
