using System;
using System.Collections.Generic;
using System.Diagnostics;
using Bmf.Core.Messaging.Inbound;

namespace Bmf.Core.Messaging;

/// <summary>
/// Injects and extracts distributed trace context in transport headers.
/// </summary>
/// <remarks>
/// The <c>traceparent</c>, <c>tracestate</c>, and baggage headers are W3C transport-propagation headers used by
/// the OpenTelemetry messaging convention. They are intentionally plain and un-prefixed, rather than CloudEvents
/// attributes. The CloudEvents Distributed Tracing extension captures creation-time provenance and is
/// intentionally deferred to a later slice.
/// </remarks>
public static class TraceContextHeaders
{
    /// <summary>
    /// Injects trace-context headers into a string-valued carrier.
    /// </summary>
    /// <param name="headers">The transport headers to populate.</param>
    /// <param name="activity">The activity to inject, or <see langword="null" /> to use <see cref="Activity.Current" />.</param>
    public static void Inject(IDictionary<string, string?> headers, Activity? activity = null)
    {
        if (headers is null)
        {
            throw new ArgumentNullException(nameof(headers));
        }

        DistributedContextPropagator.Current.Inject(
            activity ?? Activity.Current,
            headers,
            static (carrier, key, value) => ((IDictionary<string, string?>) carrier!)[key] = value
        );
    }

    /// <summary>
    /// Injects trace-context headers into an object-valued carrier.
    /// </summary>
    /// <param name="headers">The transport headers to populate.</param>
    /// <param name="activity">The activity to inject, or <see langword="null" /> to use <see cref="Activity.Current" />.</param>
    public static void Inject(IDictionary<string, object?> headers, Activity? activity = null)
    {
        if (headers is null)
        {
            throw new ArgumentNullException(nameof(headers));
        }

        DistributedContextPropagator.Current.Inject(
            activity ?? Activity.Current,
            headers,
            static (carrier, key, value) => ((IDictionary<string, object?>) carrier!)[key] = value
        );
    }

    /// <summary>
    /// Extracts W3C trace context and baggage from an inbound transport message.
    /// </summary>
    /// <param name="transportMessage">The transport message carrying inbound headers.</param>
    /// <returns>The extracted trace parent, trace state, and baggage.</returns>
    /// <remarks>
    /// This reads the W3C <c>traceparent</c>, <c>tracestate</c>, and baggage transport-propagation headers used by
    /// the OpenTelemetry messaging convention. They are distinct from, and must not be confused with,
    /// <c>cloudEvents:*</c> attributes. Extraction honours <see cref="DistributedContextPropagator.Current" />, so
    /// applications that replace the process-wide propagator affect both framework consumption and raw-consume
    /// callers. Raw-consume callers should use this method when they need trace context before the BMF inbound
    /// pipeline runs.
    /// </remarks>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="transportMessage" /> is <see langword="null" />.</exception>
    public static TraceContextHeadersExtractResult Extract(TransportMessage transportMessage)
    {
        if (transportMessage is null)
        {
            throw new ArgumentNullException(nameof(transportMessage));
        }

        var propagator = DistributedContextPropagator.Current;
        propagator.ExtractTraceIdAndState(
            transportMessage,
            GetTransportMessageHeader,
            out var traceParent,
            out var traceState
        );

        return new TraceContextHeadersExtractResult(
            traceParent,
            traceState,
            ToBaggageMap(propagator.ExtractBaggage(transportMessage, GetTransportMessageHeader))
        );
    }

    /// <summary>
    /// Extracts W3C trace context and baggage from an inbound object-valued header carrier.
    /// </summary>
    /// <param name="headers">The inbound transport headers.</param>
    /// <returns>The extracted trace parent, trace state, and baggage.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="headers" /> is <see langword="null" />.</exception>
    public static TraceContextHeadersExtractResult Extract(IReadOnlyDictionary<string, object?> headers)
    {
        if (headers is null)
        {
            throw new ArgumentNullException(nameof(headers));
        }

        var propagator = DistributedContextPropagator.Current;
        propagator.ExtractTraceIdAndState(
            headers,
            GetDictionaryHeader,
            out var traceParent,
            out var traceState
        );

        return new TraceContextHeadersExtractResult(
            traceParent,
            traceState,
            ToBaggageMap(propagator.ExtractBaggage(headers, GetDictionaryHeader))
        );
    }

    private static Dictionary<string, string?> ToBaggageMap(IEnumerable<KeyValuePair<string, string?>>? baggage)
    {
        // Materialize an owned snapshot keyed ordinally: it is the extracted value's immutable backing store, and
        // collapsing any repeated baggage key (last wins) keeps the map a faithful, deduplicated view of the wire.
        var map = new Dictionary<string, string?>(StringComparer.Ordinal);

        if (baggage is null)
        {
            return map;
        }

        foreach (var pair in baggage)
        {
            map[pair.Key] = pair.Value;
        }

        return map;
    }

    private static void GetTransportMessageHeader(
        object? carrier,
        string fieldName,
        out string? fieldValue,
        out IEnumerable<string>? fieldValues
    )
    {
        fieldValues = null;
        fieldValue = carrier is TransportMessage transportMessage &&
                     transportMessage.TryGetHeaderString(fieldName, out var value) ?
            value :
            null;
    }

    private static void GetDictionaryHeader(
        object? carrier,
        string fieldName,
        out string? fieldValue,
        out IEnumerable<string>? fieldValues
    )
    {
        fieldValues = null;
        if (carrier is not IReadOnlyDictionary<string, object?> headers ||
            !headers.TryGetValue(fieldName, out var rawValue) ||
            rawValue is null)
        {
            fieldValue = null;
            return;
        }

        fieldValue = rawValue switch
        {
            string stringValue => stringValue,
            byte[] bytes => System.Text.Encoding.UTF8.GetString(bytes),
            ReadOnlyMemory<byte> memory => System.Text.Encoding.UTF8.GetString(memory.Span),
            Memory<byte> memory => System.Text.Encoding.UTF8.GetString(memory.Span),
            IFormattable formattable => formattable.ToString(null, System.Globalization.CultureInfo.InvariantCulture),
            _ => rawValue.ToString()
        };
    }
}
