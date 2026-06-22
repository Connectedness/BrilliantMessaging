using System;
using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;

namespace Bmf.Core.Messaging.Inbound;

/// <summary>
/// Builds an ordered inbound message inspector chain for a consumer.
/// </summary>
/// <remarks>
/// Entries are evaluated in declaration order by the transport runtime. The first inspector or recognizer that
/// returns a result wins.
/// </remarks>
public sealed class InboundMessageInspectorChainBuilder
{
    private readonly List<InboundMessageInspectorChainEntry> _entries = [];

    /// <summary>
    /// Adds the default CloudEvents inspector to the chain with the requested auto-registration lifetime.
    /// </summary>
    /// <param name="serviceLifetime">The lifetime used when the inspector type is auto-registered.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="serviceLifetime" /> is not a defined value.</exception>
    public InboundMessageInspectorChainBuilder CloudEvents(ServiceLifetime serviceLifetime = ServiceLifetime.Singleton)
    {
        return Use<CloudEventsInboundMessageInspector>(serviceLifetime);
    }

    /// <summary>
    /// Adds an inspector resolved from dependency injection to the chain with the requested auto-registration
    /// lifetime.
    /// </summary>
    /// <typeparam name="TInspector">The inspector type.</typeparam>
    /// <param name="serviceLifetime">The lifetime used when the inspector type is auto-registered.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="serviceLifetime" /> is not a defined value.</exception>
    public InboundMessageInspectorChainBuilder Use<TInspector>(
        ServiceLifetime serviceLifetime = ServiceLifetime.Singleton
    )
        where TInspector : class, IInboundMessageInspector
    {
        _entries.Add(new ServiceInboundMessageInspectorChainEntry(typeof(TInspector), serviceLifetime));
        return this;
    }

    /// <summary>
    /// Starts a recognizer entry with an arbitrary predicate over the transport message.
    /// </summary>
    /// <param name="predicate">The predicate that decides whether the recognizer matches.</param>
    /// <returns>A recognizer builder that must be completed with <c>As&lt;T&gt;()</c>.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="predicate" /> is <see langword="null" />.</exception>
    public InboundMessageRecognizerBuilder When(Func<TransportMessage, bool> predicate)
    {
        return new InboundMessageRecognizerBuilder(this, predicate);
    }

    /// <summary>
    /// Starts a recognizer entry that matches when the named header is present.
    /// </summary>
    /// <param name="name">The header name.</param>
    /// <returns>A recognizer builder that must be completed with <c>As&lt;T&gt;()</c>.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="name" /> is null or whitespace.</exception>
    public InboundMessageRecognizerBuilder WhenHeader(string name)
    {
        var headerName = RequireText(name, nameof(name));
        return When(message => message.TryGetHeaderString(headerName, out _));
    }

    /// <summary>
    /// Starts a recognizer entry that matches when the named header is present and has the requested value.
    /// </summary>
    /// <param name="name">The header name.</param>
    /// <param name="value">The expected header value.</param>
    /// <returns>A recognizer builder that must be completed with <c>As&lt;T&gt;()</c>.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="name" /> or <paramref name="value" /> is null or whitespace.</exception>
    public InboundMessageRecognizerBuilder WhenHeader(string name, string value)
    {
        var headerName = RequireText(name, nameof(name));
        var expectedValue = RequireText(value, nameof(value));
        return When(
            message => message.TryGetHeaderString(headerName, out var actualValue) &&
                       string.Equals(actualValue, expectedValue, StringComparison.Ordinal)
        );
    }

    /// <summary>
    /// Starts a recognizer entry that matches when the transport content type equals the requested value.
    /// </summary>
    /// <param name="value">The expected content type.</param>
    /// <returns>A recognizer builder that must be completed with <c>As&lt;T&gt;()</c>.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="value" /> is null or whitespace.</exception>
    public InboundMessageRecognizerBuilder WhenContentType(string value)
    {
        var expectedValue = RequireText(value, nameof(value));
        return When(
            message => string.Equals(message.ContentType, expectedValue, StringComparison.OrdinalIgnoreCase)
        );
    }

    /// <summary>
    /// Builds the immutable chain entries accumulated so far.
    /// </summary>
    /// <returns>The chain entries in declaration order.</returns>
    public IReadOnlyList<InboundMessageInspectorChainEntry> Build()
    {
        return _entries.ToArray();
    }

    internal InboundMessageInspectorChainBuilder AddRecognizer(
        Func<TransportMessage, bool> predicate,
        Type messageType,
        string? explicitDiscriminator
    )
    {
        _entries.Add(new RecognizerInboundMessageInspectorChainEntry(predicate, messageType, explicitDiscriminator));
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
