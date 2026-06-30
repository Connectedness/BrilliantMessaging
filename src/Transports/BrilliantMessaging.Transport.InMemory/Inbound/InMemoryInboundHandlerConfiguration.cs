using System;
using BrilliantMessaging.Core.Messaging.Inbound;

namespace BrilliantMessaging.Transport.InMemory.Inbound;

/// <summary>
/// The compiled configuration of a single in-memory handler registration: its deserializer and acknowledgement
/// mode.
/// </summary>
/// <param name="DeserializerType">The deserializer type used to decode the message body.</param>
/// <param name="AckMode">The acknowledgement mode for the handler.</param>
public sealed record InMemoryInboundHandlerConfiguration(Type DeserializerType, MessageAckMode AckMode);
