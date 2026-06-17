using System;
using System.Collections.Generic;
using System.Linq;

namespace Bmf.Core.Messaging;

/// <summary>
/// Reports that a topology failed compile-time validation. The exception type is direction-neutral, but the
/// individual validation messages remain precise and name the failed transport feature and direction where
/// relevant, such as "RabbitMQ outbound target ...", "RabbitMQ inbound endpoint ...", "RabbitMQ exchange ...",
/// or "RabbitMQ consumer channel group ...".
/// </summary>
public sealed class TopologyValidationException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="TopologyValidationException" /> class.
    /// </summary>
    /// <param name="validationErrors">The non-empty set of validation error messages.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="validationErrors" /> is <see langword="null" />.</exception>
    /// <exception cref="ArgumentException">Thrown when <paramref name="validationErrors" /> is empty.</exception>
    public TopologyValidationException(IReadOnlyList<string> validationErrors)
        : base("Topology validation failed.")
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
