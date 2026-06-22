using System;
using System.Collections.Generic;
using System.Linq;

namespace Bmf.Core.Messaging;

/// <summary>
/// Explicitly declares stable CloudEvents contract discriminators.
/// </summary>
public sealed class MessageContractRegistryBuilder : IBuildable<IMessageContractRegistry>
{
    private readonly List<MessageContractRegistration> _registrations = [];

    /// <inheritdoc />
    /// <remarks>
    /// Validates the accumulated mappings before producing the immutable <see cref="IMessageContractRegistry" />.
    /// </remarks>
    /// <exception cref="MessageContractRegistryValidationException">
    /// Thrown when the mappings are inconsistent — for example a message type with two canonical discriminators,
    /// a discriminator that maps to two message types, or inbound aliases on a type that does not accept its
    /// canonical discriminator inbound.
    /// </exception>
    IMessageContractRegistry IBuildable<IMessageContractRegistry>.Build()
    {
        var validationErrors = Validate();

        if (validationErrors.Count > 0)
        {
            throw new MessageContractRegistryValidationException(validationErrors);
        }

        Dictionary<Type, string> discriminatorsByMessageType = new ();
        Dictionary<string, Type> messageTypesByDiscriminator = new (StringComparer.Ordinal);
        Dictionary<Type, string> dataSchemasByMessageType = new ();

        foreach (var registration in _registrations)
        {
            discriminatorsByMessageType.Add(registration.MessageType, registration.Discriminator);

            if (registration.AcceptsCanonicalInbound)
            {
                messageTypesByDiscriminator.Add(registration.Discriminator, registration.MessageType);
            }

            foreach (var alias in registration.InboundAliases)
            {
                messageTypesByDiscriminator.Add(alias, registration.MessageType);
            }

            if (registration.DataSchema is not null)
            {
                dataSchemasByMessageType.Add(registration.MessageType, registration.DataSchema);
            }
        }

        return new MessageContractRegistry(
            discriminatorsByMessageType,
            messageTypesByDiscriminator,
            dataSchemasByMessageType
        );
    }

    /// <summary>
    /// Maps a message type for publishing and consuming. The canonical discriminator is accepted inbound.
    /// </summary>
    public MessageContractMapBuilder Map<T>(string discriminator)
    {
        return Add(typeof(T), discriminator, acceptsCanonicalInbound: true);
    }

    /// <summary>
    /// Maps a message type for publishing only. The canonical discriminator is not registered inbound.
    /// </summary>
    public MessageContractMapBuilder MapOutbound<T>(string discriminator)
    {
        return Add(typeof(T), discriminator, acceptsCanonicalInbound: false);
    }

    private MessageContractMapBuilder Add(Type messageType, string discriminator, bool acceptsCanonicalInbound)
    {
        var registration = new MessageContractRegistration(
            messageType,
            RequireText(discriminator, nameof(discriminator)),
            acceptsCanonicalInbound
        );
        _registrations.Add(registration);
        return new MessageContractMapBuilder(registration);
    }

    private List<string> Validate()
    {
        List<string> validationErrors = [];

        foreach (var group in _registrations.GroupBy(static registration => registration.MessageType)
                    .Where(static group => group.Count() > 1)
                    .OrderBy(static group => GetTypeName(group.Key), StringComparer.Ordinal))
        {
            validationErrors.Add(
                $"Message type '{GetTypeName(group.Key)}' has multiple canonical CloudEvents discriminators: {FormatQuotedValues(group.Select(static registration => registration.Discriminator))}."
            );
        }

        foreach (var group in _registrations.SelectMany(GetAllDiscriminatorMappings)
                    .GroupBy(static mapping => mapping.Discriminator, StringComparer.Ordinal)
                    .Where(static group => group.Select(static mapping => mapping.MessageType).Distinct().Count() > 1)
                    .OrderBy(static group => group.Key, StringComparer.Ordinal))
        {
            validationErrors.Add(
                $"CloudEvents discriminator '{group.Key}' maps to multiple message types: {FormatQuotedValues(group.Select(static mapping => GetTypeName(mapping.MessageType)))}."
            );
        }

        foreach (var group in _registrations.SelectMany(GetInboundDiscriminatorMappings)
                    .GroupBy(
                         static mapping => new DiscriminatorMapping(mapping.Discriminator, mapping.MessageType)
                     )
                    .Where(static group => group.Count() > 1)
                    .OrderBy(static group => group.Key.Discriminator, StringComparer.Ordinal)
                    .ThenBy(static group => GetTypeName(group.Key.MessageType), StringComparer.Ordinal))
        {
            validationErrors.Add(
                $"Inbound CloudEvents discriminator '{group.Key.Discriminator}' is registered multiple times for message type '{GetTypeName(group.Key.MessageType)}'."
            );
        }

        foreach (var registration in _registrations.Where(
                         static registration => registration.InboundAliases.Count > 0 &&
                                                !registration.AcceptsCanonicalInbound
                     )
                    .OrderBy(static registration => GetTypeName(registration.MessageType), StringComparer.Ordinal))
        {
            validationErrors.Add(
                $"Message type '{GetTypeName(registration.MessageType)}' registers inbound CloudEvents discriminators but does not accept its canonical discriminator '{registration.Discriminator}' inbound."
            );
        }

        return validationErrors;
    }

    private static IEnumerable<DiscriminatorMapping> GetAllDiscriminatorMappings(
        MessageContractRegistration registration
    )
    {
        yield return new DiscriminatorMapping(registration.Discriminator, registration.MessageType);

        foreach (var alias in registration.InboundAliases)
        {
            yield return new DiscriminatorMapping(alias, registration.MessageType);
        }
    }

    private static IEnumerable<DiscriminatorMapping> GetInboundDiscriminatorMappings(
        MessageContractRegistration registration
    )
    {
        if (registration.AcceptsCanonicalInbound)
        {
            yield return new DiscriminatorMapping(registration.Discriminator, registration.MessageType);
        }

        foreach (var alias in registration.InboundAliases)
        {
            yield return new DiscriminatorMapping(alias, registration.MessageType);
        }
    }

    private static string FormatQuotedValues(IEnumerable<string> values)
    {
        return string.Join(
            ", ",
            values.Distinct(StringComparer.Ordinal).OrderBy(static value => value, StringComparer.Ordinal)
               .Select(static value => $"'{value}'")
        );
    }

    private static string GetTypeName(Type messageType)
    {
        return messageType.FullName ?? messageType.Name;
    }

    private static string RequireText(string value, string parameterName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", parameterName);
        }

        return value;
    }

    private sealed record DiscriminatorMapping(string Discriminator, Type MessageType);
}
