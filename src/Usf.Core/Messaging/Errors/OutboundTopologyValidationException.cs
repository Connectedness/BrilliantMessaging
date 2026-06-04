using System;
using System.Collections.Generic;
using System.Linq;

namespace Usf.Core.Messaging.Errors;

public sealed class OutboundTopologyValidationException : Exception
{
    public OutboundTopologyValidationException(IReadOnlyList<string> validationErrors)
        : base("Outbound topology validation failed.")
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

    public IReadOnlyList<string> ValidationErrors { get; }
}
