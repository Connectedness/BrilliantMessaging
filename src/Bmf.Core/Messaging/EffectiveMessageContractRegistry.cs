using System;
using System.Collections.Generic;
using System.Linq;

namespace Bmf.Core.Messaging;

/// <summary>
/// An <see cref="IMessageContractRegistry" /> that layers a transport-specific dialect over the canonical
/// registry: dialect mappings take precedence, falling back to the canonical registry when the dialect has no
/// entry. This lets a transport register additional or overriding discriminators without mutating the shared
/// canonical contracts.
/// </summary>
public sealed class EffectiveMessageContractRegistry : IMessageContractRegistry
{
    private readonly IMessageContractRegistry _canonical;
    private readonly MessageContractRegistry _dialect;

    /// <summary>
    /// Initializes a new instance of the <see cref="EffectiveMessageContractRegistry" /> class.
    /// </summary>
    /// <param name="canonical">The shared canonical registry consulted when the dialect has no entry.</param>
    /// <param name="dialect">The transport-specific registry whose entries take precedence.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="canonical" /> or <paramref name="dialect" /> is <see langword="null" />.</exception>
    public EffectiveMessageContractRegistry(IMessageContractRegistry canonical, MessageContractRegistry dialect)
    {
        _canonical = canonical ?? throw new ArgumentNullException(nameof(canonical));
        _dialect = dialect ?? throw new ArgumentNullException(nameof(dialect));
    }

    /// <inheritdoc />
    public string GetDiscriminator(Type messageType)
    {
        return _dialect.TryGetDiscriminator(messageType, out var discriminator) ?
            discriminator! :
            _canonical.GetDiscriminator(messageType);
    }

    /// <inheritdoc />
    public bool TryGetDiscriminator(Type messageType, out string? discriminator)
    {
        return _dialect.TryGetDiscriminator(messageType, out discriminator) ||
               _canonical.TryGetDiscriminator(messageType, out discriminator);
    }

    /// <inheritdoc />
    public string? GetDataSchema(Type messageType)
    {
        return _dialect.TryGetDataSchema(messageType, out var dataSchema) ?
            dataSchema :
            _canonical.GetDataSchema(messageType);
    }

    /// <inheritdoc />
    public IReadOnlyCollection<string> GetInboundDiscriminators(Type messageType)
    {
        if (messageType is null)
        {
            throw new ArgumentNullException(nameof(messageType));
        }

        return _dialect.GetInboundDiscriminators(messageType)
           .Concat(_canonical.GetInboundDiscriminators(messageType))
           .Distinct(StringComparer.Ordinal)
           .OrderBy(static discriminator => discriminator, StringComparer.Ordinal)
           .ToArray();
    }

    /// <inheritdoc />
    public bool TryResolveType(string discriminator, out Type? messageType)
    {
        return _dialect.TryResolveType(discriminator, out messageType) ||
               _canonical.TryResolveType(discriminator, out messageType);
    }
}
