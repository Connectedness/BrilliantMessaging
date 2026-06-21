using System.Collections.Generic;

namespace Bmf.Core.Messaging;

/// <summary>
/// The trace parent, trace state, and baggage extracted from inbound transport headers.
/// </summary>
/// <param name="TraceParent">The extracted W3C <c>traceparent</c> value, or <see langword="null" /> when absent.</param>
/// <param name="TraceState">The extracted W3C <c>tracestate</c> value, or <see langword="null" /> when absent.</param>
/// <param name="Baggage">The extracted baggage key-value pairs.</param>
public sealed record TraceContextHeadersExtractResult(
    string? TraceParent,
    string? TraceState,
    IEnumerable<KeyValuePair<string, string?>> Baggage
);
