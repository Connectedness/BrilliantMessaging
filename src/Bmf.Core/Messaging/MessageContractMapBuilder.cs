using System;

namespace Bmf.Core.Messaging;

/// <summary>
/// Refines a single message-contract mapping produced by <see cref="MessageContractRegistryBuilder.Map{T}" />
/// or <see cref="MessageContractRegistryBuilder.MapOutbound{T}" />, adding inbound aliases and a data schema.
/// </summary>
public sealed class MessageContractMapBuilder
{
    private readonly MessageContractRegistration _registration;

    internal MessageContractMapBuilder(MessageContractRegistration registration)
    {
        _registration = registration;
    }

    /// <summary>
    /// Registers an additional inbound discriminator (alias) that maps to the same message type, allowing the
    /// consumer to accept messages published under a legacy or alternative <c>type</c> value.
    /// </summary>
    /// <param name="discriminator">The alias discriminator to accept inbound.</param>
    /// <returns>The same <see cref="MessageContractMapBuilder" /> instance for chaining.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="discriminator" /> is null or whitespace.</exception>
    public MessageContractMapBuilder WithInboundAlias(string discriminator)
    {
        _registration.InboundAliases.Add(RequireText(discriminator, nameof(discriminator)));
        return this;
    }

    /// <summary>
    /// Sets the CloudEvents <c>dataschema</c> attribute attached to messages of this type.
    /// </summary>
    /// <param name="dataSchema">A URI-reference identifying the data schema.</param>
    /// <returns>The same <see cref="MessageContractMapBuilder" /> instance for chaining.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="dataSchema" /> is null, whitespace, or not a valid URI-reference.</exception>
    public MessageContractMapBuilder WithDataSchema(string dataSchema)
    {
        var value = RequireText(dataSchema, nameof(dataSchema));

        if (!Uri.TryCreate(value, UriKind.RelativeOrAbsolute, out _))
        {
            throw new ArgumentException("The value must be a URI-reference.", nameof(dataSchema));
        }

        _registration.DataSchema = value;
        return this;
    }

    private static string RequireText(string value, string parameterName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", parameterName);
        }

        return value;
    }
}
