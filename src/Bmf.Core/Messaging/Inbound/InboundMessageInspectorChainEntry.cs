using System;
using Microsoft.Extensions.DependencyInjection;

namespace Bmf.Core.Messaging.Inbound;

/// <summary>
/// Describes one declaration-time entry in an inbound message inspector chain.
/// </summary>
public abstract class InboundMessageInspectorChainEntry
{
    private protected InboundMessageInspectorChainEntry() { }
}

/// <summary>
/// Describes an inspector chain entry resolved from dependency injection.
/// </summary>
public sealed class ServiceInboundMessageInspectorChainEntry : InboundMessageInspectorChainEntry
{
    /// <summary>
    /// Initializes a new instance of the <see cref="ServiceInboundMessageInspectorChainEntry" /> class.
    /// </summary>
    /// <param name="inspectorType">The inspector type resolved from dependency injection.</param>
    /// <param name="serviceLifetime">The lifetime used when the inspector type is auto-registered.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="inspectorType" /> is <see langword="null" />.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="serviceLifetime" /> is not a defined value.</exception>
    public ServiceInboundMessageInspectorChainEntry(
        Type inspectorType,
        ServiceLifetime serviceLifetime = ServiceLifetime.Singleton
    )
    {
        if (!Enum.IsDefined(typeof(ServiceLifetime), serviceLifetime))
        {
            throw new ArgumentOutOfRangeException(
                nameof(serviceLifetime),
                serviceLifetime,
                "Unsupported service lifetime."
            );
        }

        InspectorType = inspectorType ?? throw new ArgumentNullException(nameof(inspectorType));
        ServiceLifetime = serviceLifetime;
    }

    /// <summary>
    /// Gets the inspector type resolved from dependency injection.
    /// </summary>
    public Type InspectorType { get; }

    /// <summary>
    /// Gets the lifetime used when the inspector type is auto-registered.
    /// </summary>
    public ServiceLifetime ServiceLifetime { get; }
}

/// <summary>
/// Describes a predicate-based recognizer entry whose discriminator may be resolved later during topology
/// compilation.
/// </summary>
public sealed class RecognizerInboundMessageInspectorChainEntry : InboundMessageInspectorChainEntry
{
    /// <summary>
    /// Initializes a new instance of the <see cref="RecognizerInboundMessageInspectorChainEntry" /> class.
    /// </summary>
    /// <param name="predicate">The predicate that decides whether the recognizer matches.</param>
    /// <param name="messageType">The message type returned when the recognizer matches.</param>
    /// <param name="explicitDiscriminator">
    /// The explicit discriminator to return, or <see langword="null" /> to resolve it from the message contract
    /// registry.
    /// </param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="predicate" /> or <paramref name="messageType" /> is <see langword="null" />.</exception>
    /// <exception cref="ArgumentException">Thrown when <paramref name="explicitDiscriminator" /> is empty or whitespace.</exception>
    public RecognizerInboundMessageInspectorChainEntry(
        Func<TransportMessage, bool> predicate,
        Type messageType,
        string? explicitDiscriminator = null
    )
    {
        if (explicitDiscriminator is not null && string.IsNullOrWhiteSpace(explicitDiscriminator))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(explicitDiscriminator));
        }

        Predicate = predicate ?? throw new ArgumentNullException(nameof(predicate));
        MessageType = messageType ?? throw new ArgumentNullException(nameof(messageType));
        ExplicitDiscriminator = explicitDiscriminator;
    }

    /// <summary>
    /// Gets the predicate that decides whether the recognizer matches.
    /// </summary>
    public Func<TransportMessage, bool> Predicate { get; }

    /// <summary>
    /// Gets the message type returned when the recognizer matches.
    /// </summary>
    public Type MessageType { get; }

    /// <summary>
    /// Gets the explicit discriminator returned when the recognizer matches, or <see langword="null" /> when the
    /// discriminator should be resolved from the message contract registry.
    /// </summary>
    public string? ExplicitDiscriminator { get; }
}
