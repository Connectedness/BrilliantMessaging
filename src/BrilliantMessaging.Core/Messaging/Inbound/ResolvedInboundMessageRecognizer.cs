using System;
using BrilliantMessaging.GuardClauses;

namespace BrilliantMessaging.Core.Messaging.Inbound;

/// <summary>
/// A recognizer chain entry together with the discriminator it resolves to on its topology.
/// </summary>
public sealed class ResolvedInboundMessageRecognizer
{
    /// <summary>
    /// Initializes a new instance of the <see cref="ResolvedInboundMessageRecognizer" /> class.
    /// </summary>
    /// <param name="entry">The declared recognizer entry.</param>
    /// <param name="discriminator">The resolved discriminator.</param>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="entry" /> or <paramref name="discriminator" /> is <see langword="null" />.
    /// </exception>
    /// <exception cref="BrilliantMessaging.GuardClauses.Exceptions.EmptyStringException">
    /// Thrown when <paramref name="discriminator" /> is empty.
    /// </exception>
    /// <exception cref="BrilliantMessaging.GuardClauses.Exceptions.WhiteSpaceStringException">
    /// Thrown when <paramref name="discriminator" /> contains only whitespace.
    /// </exception>
    public ResolvedInboundMessageRecognizer(RecognizerInboundMessageInspectorChainEntry entry, string discriminator)
    {
        Entry = entry.MustNotBeNull();
        Discriminator = discriminator.MustNotBeNullOrWhiteSpace();
    }

    /// <summary>
    /// Gets the declared recognizer entry.
    /// </summary>
    public RecognizerInboundMessageInspectorChainEntry Entry { get; }

    /// <summary>
    /// Gets the resolved discriminator.
    /// </summary>
    public string Discriminator { get; }
}
