using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

namespace Bmf.Core.Messaging;

/// <summary>
/// The default immutable <see cref="IMessageContractRegistry" />, built from the type-to-discriminator,
/// discriminator-to-type, and type-to-data-schema mappings produced by <see cref="MessageContractRegistryBuilder" />.
/// </summary>
public sealed class MessageContractRegistry : IMessageContractRegistry
{
    private readonly IReadOnlyDictionary<Type, string> _dataSchemasByMessageType;
    private readonly IReadOnlyDictionary<Type, string> _discriminatorsByMessageType;
    private readonly IReadOnlyDictionary<string, Type> _messageTypesByDiscriminator;

    /// <summary>
    /// Initializes a new instance of the <see cref="MessageContractRegistry" /> class. The supplied dictionaries
    /// are copied, so later mutation of the originals does not affect the registry.
    /// </summary>
    /// <param name="discriminatorsByMessageType">The canonical outbound discriminator for each message type.</param>
    /// <param name="messageTypesByDiscriminator">The message type each inbound discriminator (canonical or alias) maps to.</param>
    /// <param name="dataSchemasByMessageType">The data schema for each message type that declares one.</param>
    /// <exception cref="ArgumentNullException">Thrown when any argument is <see langword="null" />.</exception>
    public MessageContractRegistry(
        IDictionary<Type, string> discriminatorsByMessageType,
        IDictionary<string, Type> messageTypesByDiscriminator,
        IDictionary<Type, string> dataSchemasByMessageType
    )
    {
        if (discriminatorsByMessageType is null)
        {
            throw new ArgumentNullException(nameof(discriminatorsByMessageType));
        }

        if (messageTypesByDiscriminator is null)
        {
            throw new ArgumentNullException(nameof(messageTypesByDiscriminator));
        }

        if (dataSchemasByMessageType is null)
        {
            throw new ArgumentNullException(nameof(dataSchemasByMessageType));
        }

        _discriminatorsByMessageType = new ReadOnlyDictionary<Type, string>(
            new Dictionary<Type, string>(discriminatorsByMessageType)
        );
        _messageTypesByDiscriminator = new ReadOnlyDictionary<string, Type>(
            new Dictionary<string, Type>(messageTypesByDiscriminator, StringComparer.Ordinal)
        );
        _dataSchemasByMessageType = new ReadOnlyDictionary<Type, string>(
            new Dictionary<Type, string>(dataSchemasByMessageType)
        );
        RegisteredMessageTypes = _discriminatorsByMessageType.Keys.OrderBy(
            static messageType => messageType.FullName ?? messageType.Name,
            StringComparer.Ordinal
        ).ToArray();
    }

    /// <summary>
    /// Gets the registered message types, ordered by full type name.
    /// </summary>
    public IReadOnlyCollection<Type> RegisteredMessageTypes { get; }

    /// <inheritdoc />
    public string GetDiscriminator(Type messageType)
    {
        if (messageType is null)
        {
            throw new ArgumentNullException(nameof(messageType));
        }

        if (!_discriminatorsByMessageType.TryGetValue(messageType, out var discriminator))
        {
            throw new MessageContractNotRegisteredException(messageType);
        }

        return discriminator;
    }

    /// <inheritdoc />
    public bool TryGetDiscriminator(Type messageType, out string? discriminator)
    {
        if (messageType is null)
        {
            throw new ArgumentNullException(nameof(messageType));
        }

        return _discriminatorsByMessageType.TryGetValue(messageType, out discriminator);
    }

    /// <inheritdoc />
    public string? GetDataSchema(Type messageType)
    {
        if (messageType is null)
        {
            throw new ArgumentNullException(nameof(messageType));
        }

        return _dataSchemasByMessageType.TryGetValue(messageType, out var dataSchema) ? dataSchema : null;
    }

    /// <inheritdoc />
    public IReadOnlyCollection<string> GetInboundDiscriminators(Type messageType)
    {
        if (messageType is null)
        {
            throw new ArgumentNullException(nameof(messageType));
        }

        return _messageTypesByDiscriminator
           .Where(pair => pair.Value == messageType)
           .Select(static pair => pair.Key)
           .OrderBy(static discriminator => discriminator, StringComparer.Ordinal)
           .ToArray();
    }

    /// <inheritdoc />
    public bool TryResolveType(string discriminator, out Type? messageType)
    {
        if (string.IsNullOrWhiteSpace(discriminator))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(discriminator));
        }

        return _messageTypesByDiscriminator.TryGetValue(discriminator, out messageType);
    }

    /// <summary>
    /// Attempts to get the data schema registered for the given message type.
    /// </summary>
    /// <param name="messageType">The message type to resolve.</param>
    /// <param name="dataSchema">When this method returns, the registered data schema, or <see langword="null" /> when none was registered.</param>
    /// <returns><see langword="true" /> when a data schema is registered; otherwise <see langword="false" />.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="messageType" /> is <see langword="null" />.</exception>
    public bool TryGetDataSchema(Type messageType, out string? dataSchema)
    {
        if (messageType is null)
        {
            throw new ArgumentNullException(nameof(messageType));
        }

        return _dataSchemasByMessageType.TryGetValue(messageType, out dataSchema);
    }
}
