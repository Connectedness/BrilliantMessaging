using System;
using System.Collections.Generic;
using System.Linq;

namespace Bmf.Core.Messaging;

/// <summary>
/// Thrown when building a <see cref="MessageContractRegistry" /> fails because the accumulated mappings are
/// inconsistent. The individual problems are exposed through <see cref="ValidationErrors" />.
/// </summary>
public sealed class MessageContractRegistryValidationException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="MessageContractRegistryValidationException" /> class.
    /// </summary>
    /// <param name="validationErrors">The non-empty set of validation error messages.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="validationErrors" /> is <see langword="null" />.</exception>
    /// <exception cref="ArgumentException">Thrown when <paramref name="validationErrors" /> is empty.</exception>
    public MessageContractRegistryValidationException(IReadOnlyList<string> validationErrors)
        : base("Message contract registry validation failed.")
    {
        if (validationErrors is null)
        {
            throw new ArgumentNullException(nameof(validationErrors));
        }

        if (validationErrors.Count == 0)
        {
            throw new ArgumentException("At least one validation error must be provided.", nameof(validationErrors));
        }

        ValidationErrors = Array.AsReadOnly(
            validationErrors.OrderBy(static error => error, StringComparer.Ordinal).ToArray()
        );
    }

    /// <summary>
    /// Gets the validation error messages, ordered for stable reporting.
    /// </summary>
    public IReadOnlyList<string> ValidationErrors { get; }
}
