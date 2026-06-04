using System;

namespace Usf.Core.Messaging;

public sealed class MessageContractMapBuilder
{
    private readonly MessageContractRegistration _registration;

    internal MessageContractMapBuilder(MessageContractRegistration registration)
    {
        _registration = registration;
    }

    public MessageContractMapBuilder WithInboundAlias(string discriminator)
    {
        _registration.InboundAliases.Add(RequireText(discriminator, nameof(discriminator)));
        return this;
    }

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
