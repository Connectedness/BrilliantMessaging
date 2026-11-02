using System;
using System.Collections.Immutable;
using BrilliantMessaging.GuardClauses;
using Microsoft.Extensions.DependencyInjection;
using ImmutableArrayType =
    System.Collections.Immutable.ImmutableArray<
        BrilliantMessaging.Core.Messaging.Inbound.InboundMessageInspectorChainEntry>;

namespace BrilliantMessaging.Core.Messaging.Inbound;

/// <summary>
/// Builds an ordered inbound message inspector chain for a consumer.
/// </summary>
/// <remarks>
/// Entries are evaluated in declaration order by the transport runtime. The first inspector or recognizer that
/// returns a result wins.
/// </remarks>
public sealed class InboundMessageInspectorChainBuilder : IBuildable<ImmutableArrayType>
{
    private readonly ImmutableArrayType.Builder _entries =
        ImmutableArray.CreateBuilder<InboundMessageInspectorChainEntry>();

    /// <inheritdoc />
    ImmutableArrayType IBuildable<ImmutableArrayType>.Build()
    {
        return _entries.ToImmutable();
    }

    /// <summary>
    /// Adds the default CloudEvents inspector to the chain with the requested auto-registration lifetime.
    /// </summary>
    /// <remarks>
    /// Topology compilation binds this entry to an inspector instance that uses the topology's effective message
    /// contract registry, so <paramref name="serviceLifetime" /> and any custom dependency-injection registration of
    /// <see cref="CloudEventsInboundMessageInspector" /> are ignored at runtime.
    /// </remarks>
    /// <param name="serviceLifetime">The lifetime used when the inspector type is auto-registered.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="BrilliantMessaging.GuardClauses.Exceptions.EnumValueNotDefinedException">
    /// Thrown when <paramref name="serviceLifetime" /> is not a
    /// defined value.
    /// </exception>
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
    /// <exception cref="BrilliantMessaging.GuardClauses.Exceptions.EnumValueNotDefinedException">
    /// Thrown when <paramref name="serviceLifetime" /> is not a
    /// defined value.
    /// </exception>
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
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="name" /> is <see langword="null" />.
    /// </exception>
    /// <exception cref="BrilliantMessaging.GuardClauses.Exceptions.EmptyStringException">
    /// Thrown when <paramref name="name" /> is empty.
    /// </exception>
    /// <exception cref="BrilliantMessaging.GuardClauses.Exceptions.WhiteSpaceStringException">
    /// Thrown when <paramref name="name" /> contains only whitespace.
    /// </exception>
    public InboundMessageRecognizerBuilder WhenHeader(string name)
    {
        var headerName = name.MustNotBeNullOrWhiteSpace();
        return When(message => message.TryGetHeaderString(headerName, out _));
    }

    /// <summary>
    /// Starts a recognizer entry that matches when the named header is present and has the requested value.
    /// </summary>
    /// <param name="name">The header name.</param>
    /// <param name="value">The expected header value.</param>
    /// <returns>A recognizer builder that must be completed with <c>As&lt;T&gt;()</c>.</returns>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="name" /> or <paramref name="value" /> is <see langword="null" />.
    /// </exception>
    /// <exception cref="BrilliantMessaging.GuardClauses.Exceptions.EmptyStringException">
    /// Thrown when <paramref name="name" /> or <paramref name="value" /> is empty.
    /// </exception>
    /// <exception cref="BrilliantMessaging.GuardClauses.Exceptions.WhiteSpaceStringException">
    /// Thrown when <paramref name="name" /> or <paramref name="value" /> contains only whitespace.
    /// </exception>
    public InboundMessageRecognizerBuilder WhenHeader(string name, string value)
    {
        var headerName = name.MustNotBeNullOrWhiteSpace();
        var expectedValue = value.MustNotBeNullOrWhiteSpace();
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
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="value" /> is <see langword="null" />.
    /// </exception>
    /// <exception cref="BrilliantMessaging.GuardClauses.Exceptions.EmptyStringException">
    /// Thrown when <paramref name="value" /> is empty.
    /// </exception>
    /// <exception cref="BrilliantMessaging.GuardClauses.Exceptions.WhiteSpaceStringException">
    /// Thrown when <paramref name="value" /> contains only whitespace.
    /// </exception>
    public InboundMessageRecognizerBuilder WhenContentType(string value)
    {
        var expectedValue = value.MustNotBeNullOrWhiteSpace();
        return When(
            message => string.Equals(message.ContentType, expectedValue, StringComparison.OrdinalIgnoreCase)
        );
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
}
