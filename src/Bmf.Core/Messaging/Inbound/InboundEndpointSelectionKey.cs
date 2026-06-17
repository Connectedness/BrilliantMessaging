namespace Bmf.Core.Messaging.Inbound;

/// <summary>
/// The key used to select an inbound endpoint for a delivery: the combination of the transport source and the
/// CloudEvents discriminator a message arrived with.
/// </summary>
/// <param name="Source">The transport source (for example a queue name) the message arrived from.</param>
/// <param name="Discriminator">The CloudEvents <c>type</c> discriminator the message carries.</param>
public readonly record struct InboundEndpointSelectionKey(string Source, string Discriminator);
