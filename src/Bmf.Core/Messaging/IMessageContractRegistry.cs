using System;
using System.Collections.Generic;

namespace Bmf.Core.Messaging;

/// <summary>
/// Resolves CloudEvents type discriminators without coupling wire contracts to CLR type names.
/// </summary>
/// <remarks>
/// The mapping is intentionally asymmetric: serialization resolves one canonical discriminator per CLR type,
/// while deserialization may resolve multiple discriminators to one CLR type. This permits old wire names to
/// remain accepted after a backwards-compatible rename.
/// </remarks>
public interface IMessageContractRegistry
{
    /// <summary>
    /// Gets the canonical outbound discriminator for the given message type.
    /// </summary>
    /// <param name="messageType">The message type to resolve.</param>
    /// <returns>The canonical CloudEvents <c>type</c> discriminator.</returns>
    /// <exception cref="MessageContractNotRegisteredException">Thrown when <paramref name="messageType" /> is not registered.</exception>
    string GetDiscriminator(Type messageType);

    /// <summary>
    /// Attempts to get the canonical outbound discriminator for the given message type.
    /// </summary>
    /// <param name="messageType">The message type to resolve.</param>
    /// <param name="discriminator">When this method returns, the discriminator, or <see langword="null" /> when the type is not registered.</param>
    /// <returns><see langword="true" /> when the type is registered; otherwise <see langword="false" />.</returns>
    bool TryGetDiscriminator(Type messageType, out string? discriminator);

    /// <summary>
    /// Gets the data schema registered for the given message type, if any.
    /// </summary>
    /// <param name="messageType">The message type to resolve.</param>
    /// <returns>The registered data schema, or <see langword="null" /> when none was registered.</returns>
    string? GetDataSchema(Type messageType);

    /// <summary>
    /// Gets all discriminators accepted inbound for the given message type (its canonical discriminator and any
    /// registered aliases).
    /// </summary>
    /// <param name="messageType">The message type to resolve.</param>
    /// <returns>The set of accepted inbound discriminators.</returns>
    IReadOnlyCollection<string> GetInboundDiscriminators(Type messageType);

    /// <summary>
    /// Attempts to resolve the message type that an inbound discriminator maps to.
    /// </summary>
    /// <param name="discriminator">The inbound CloudEvents <c>type</c> discriminator.</param>
    /// <param name="messageType">When this method returns, the resolved message type, or <see langword="null" /> when the discriminator is unknown.</param>
    /// <returns><see langword="true" /> when the discriminator is registered; otherwise <see langword="false" />.</returns>
    bool TryResolveType(string discriminator, out Type? messageType);
}
