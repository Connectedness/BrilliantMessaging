using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.GuardClauses;
using Microsoft.Extensions.DependencyInjection;

namespace BrilliantMessaging.Core.Messaging.Inbound;

/// <summary>
/// The compiled inspector chain for a transport's inbound consumer.
/// </summary>
/// <remarks>
/// This is the compiled, service-provider-aware counterpart of
/// <see cref="CompositeInboundMessageInspector" />: both evaluate inspectors in declaration order and return the
/// first non-<see langword="null" /> result, but this chain may resolve some entries from the per-delivery
/// <see cref="IServiceProvider" /> (honouring a scoped or transient inspector lifetime) instead of holding only
/// fixed instances. A single instance is built once at topology-compile time and shared across concurrent
/// deliveries, so it stays stateless and threads the service provider through each evaluation rather than capturing
/// it; that is why the first-match loop is not shared with <see cref="CompositeInboundMessageInspector" />.
/// </remarks>
public sealed class CompiledInboundMessageInspectorChain
{
    /// <summary>
    /// Initializes a new instance of the <see cref="CompiledInboundMessageInspectorChain" /> class.
    /// </summary>
    /// <param name="entries">The compiled chain entries in evaluation order.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="entries" /> is <see langword="null" />.</exception>
    /// <exception cref="BrilliantMessaging.GuardClauses.Exceptions.ExistingItemException">Thrown when an entry is <see langword="null" />.</exception>
    public CompiledInboundMessageInspectorChain(IReadOnlyList<CompiledInboundMessageInspectorChainEntry> entries)
    {
        Entries = entries.MustNotBeNull().MustNotContainNull(nameof(entries));
    }

    /// <summary>
    /// Gets the compiled chain entries in evaluation order.
    /// </summary>
    public IReadOnlyList<CompiledInboundMessageInspectorChainEntry> Entries { get; }

    /// <summary>
    /// Inspects a delivery by evaluating each chain entry in order and returning the first result.
    /// </summary>
    /// <param name="serviceProvider">The per-delivery service provider used for DI-resolved inspectors.</param>
    /// <param name="transportMessage">The transport message to inspect.</param>
    /// <param name="cancellationToken">A token to observe while inspecting.</param>
    /// <returns>The first inspection result, or <see langword="null" /> when no entry recognizes the delivery.</returns>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="serviceProvider" /> or <paramref name="transportMessage" /> is
    /// <see langword="null" />.
    /// </exception>
    public async ValueTask<InboundMessageInspectionResult?> InspectAsync(
        IServiceProvider serviceProvider,
        TransportMessage transportMessage,
        CancellationToken cancellationToken = default
    )
    {
        serviceProvider.MustNotBeNull();

        transportMessage.MustNotBeNull();

        foreach (var entry in Entries)
        {
            var result = await entry
               .InspectAsync(serviceProvider, transportMessage, cancellationToken)
               .ConfigureAwait(false);

            if (result is not null)
            {
                return result;
            }
        }

        return null;
    }
}

/// <summary>
/// A compiled inspector-chain entry.
/// </summary>
public abstract class CompiledInboundMessageInspectorChainEntry
{
    private protected CompiledInboundMessageInspectorChainEntry() { }

    /// <summary>
    /// Inspects a delivery through this chain entry.
    /// </summary>
    /// <param name="serviceProvider">The per-delivery service provider.</param>
    /// <param name="transportMessage">The transport message to inspect.</param>
    /// <param name="cancellationToken">A token to observe while inspecting.</param>
    /// <returns>The inspection result, or <see langword="null" /> when this entry does not recognize the delivery.</returns>
    public abstract ValueTask<InboundMessageInspectionResult?> InspectAsync(
        IServiceProvider serviceProvider,
        TransportMessage transportMessage,
        CancellationToken cancellationToken = default
    );
}

/// <summary>
/// A compiled inspector-chain entry that resolves an inspector type from dependency injection for each delivery.
/// </summary>
public sealed class CompiledServiceInboundMessageInspectorChainEntry : CompiledInboundMessageInspectorChainEntry
{
    /// <summary>
    /// Initializes a new instance of the <see cref="CompiledServiceInboundMessageInspectorChainEntry" /> class.
    /// </summary>
    /// <param name="inspectorType">The inspector type to resolve from dependency injection.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="inspectorType" /> is <see langword="null" />.</exception>
    /// <exception cref="ArgumentException">Thrown when <paramref name="inspectorType" /> does not implement <see cref="IInboundMessageInspector" />.</exception>
    public CompiledServiceInboundMessageInspectorChainEntry(Type inspectorType)
    {
        InspectorType = inspectorType
           .MustBeAssignableTo(typeof(IInboundMessageInspector))
           .MustBeConcreteClass(nameof(inspectorType));
    }

    /// <summary>
    /// Gets the inspector type resolved from dependency injection.
    /// </summary>
    public Type InspectorType { get; }

    /// <inheritdoc />
    public override ValueTask<InboundMessageInspectionResult?> InspectAsync(
        IServiceProvider serviceProvider,
        TransportMessage transportMessage,
        CancellationToken cancellationToken = default
    )
    {
        serviceProvider.MustNotBeNull();

        var inspector = (IInboundMessageInspector) serviceProvider.GetRequiredService(InspectorType);
        return inspector.InspectAsync(transportMessage, cancellationToken);
    }
}

/// <summary>
/// A compiled inspector-chain entry that reuses an already-created inspector instance.
/// </summary>
public sealed class CompiledInstanceInboundMessageInspectorChainEntry : CompiledInboundMessageInspectorChainEntry
{
    /// <summary>
    /// Initializes a new instance of the <see cref="CompiledInstanceInboundMessageInspectorChainEntry" /> class.
    /// </summary>
    /// <param name="inspector">The inspector instance to reuse.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="inspector" /> is <see langword="null" />.</exception>
    public CompiledInstanceInboundMessageInspectorChainEntry(IInboundMessageInspector inspector)
    {
        Inspector = inspector.MustNotBeNull();
    }

    /// <summary>
    /// Gets the reused inspector instance.
    /// </summary>
    public IInboundMessageInspector Inspector { get; }

    /// <inheritdoc />
    public override ValueTask<InboundMessageInspectionResult?> InspectAsync(
        IServiceProvider serviceProvider,
        TransportMessage transportMessage,
        CancellationToken cancellationToken = default
    )
    {
        return Inspector.InspectAsync(transportMessage, cancellationToken);
    }
}
