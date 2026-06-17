using System;

namespace Bmf.Core.Messaging.Outbound;

/// <summary>
/// Thrown when a required CloudEvents attribute is missing or invalid when publishing a message — for example a
/// message that neither implements <c>ICloudEvent</c> nor supplies explicit metadata. The message includes
/// instructions for supplying the attribute.
/// </summary>
public sealed class CloudEventMetadataException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="CloudEventMetadataException" /> class.
    /// </summary>
    /// <param name="attributeName">The name of the missing or invalid CloudEvents attribute.</param>
    /// <param name="supplyInstructions">Guidance on how to supply the attribute, appended to the message.</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="attributeName" /> or <paramref name="supplyInstructions" /> is null or whitespace.</exception>
    public CloudEventMetadataException(string attributeName, string supplyInstructions)
        : base(
            $"CloudEvents attribute '{RequireText(attributeName, nameof(attributeName))}' is missing or invalid. {RequireText(supplyInstructions, nameof(supplyInstructions))}"
        )
    {
        AttributeName = attributeName;
    }

    /// <summary>
    /// Gets the name of the missing or invalid CloudEvents attribute.
    /// </summary>
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
