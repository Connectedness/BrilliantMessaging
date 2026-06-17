using System;
using Bmf.Abstractions;

namespace Bmf.Core.Messaging.Outbound;

/// <summary>
/// Validates the CloudEvents <c>source</c> attribute, used both by the options <c>ValidateOnStart</c> guard and
/// at publish time when resolving the effective source.
/// </summary>
public static class CloudEventsOptionsValidation
{
    /// <summary>
    /// Gets the configured source from the options, throwing when it is missing or invalid.
    /// </summary>
    /// <param name="options">The CloudEvents options.</param>
    /// <returns>The valid source.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="options" /> is <see langword="null" />.</exception>
    /// <exception cref="CloudEventMetadataException">Thrown when the source is missing or not a valid URI-reference.</exception>
    public static string GetRequiredSource(CloudEventsOptions options)
    {
        if (options is null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        return GetRequiredSource(options.Source);
    }

    /// <summary>
    /// Validates a source value, throwing when it is missing or invalid.
    /// </summary>
    /// <param name="source">The source value to validate.</param>
    /// <returns>The valid source.</returns>
    /// <exception cref="CloudEventMetadataException">Thrown when <paramref name="source" /> is missing or not a valid URI-reference.</exception>
    public static string GetRequiredSource(string? source)
    {
        if (!IsValidSource(source))
        {
            throw new CloudEventMetadataException(
                CloudEventAttributeNames.Source,
                "Configure CloudEventsOptions.Source with a non-empty URI-reference or pass a per-call CloudEventMetadata.Source override."
            );
        }

        return source!;
    }

    /// <summary>
    /// Determines whether the given source is a non-empty URI-reference.
    /// </summary>
    /// <param name="source">The source value to check.</param>
    /// <returns><see langword="true" /> when the source is valid; otherwise <see langword="false" />.</returns>
    public static bool IsValidSource(string? source)
    {
        return !string.IsNullOrWhiteSpace(source) && Uri.TryCreate(source, UriKind.RelativeOrAbsolute, out _);
    }
}
