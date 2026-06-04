using System;

namespace Usf.Core.Messaging.Errors;

public sealed class CloudEventMetadataException : Exception
{
    public CloudEventMetadataException(string attributeName, string supplyInstructions)
        : base(
            $"CloudEvents attribute '{RequireText(attributeName, nameof(attributeName))}' is missing or invalid. {RequireText(supplyInstructions, nameof(supplyInstructions))}"
        )
    {
        AttributeName = attributeName;
    }

    public string AttributeName { get; }

    private static string RequireText(string value, string parameterName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", parameterName);
        }

        return value;
    }
}
