using System;
using System.Collections.Frozen;
using System.Collections.Immutable;
using Bmf.Core.Messaging.Inbound;
using Bmf.Core.Messaging.Outbound;

namespace Bmf.Core.Messaging;

/// <summary>
/// The immutable base for a compiled transport topology. It stores outbound targets indexed by name and message
/// type, and inbound endpoints indexed by name.
/// </summary>
public abstract class Topology
{
    /// <summary>
    /// The name of the default topology, used when no topology name is specified.
    /// </summary>
    public const string DefaultName = "default";

    private readonly FrozenDictionary<string, InboundEndpoint> _endpointsByName;
    private readonly FrozenDictionary<Type, OutboundTarget> _targetsByMessageType;
    private readonly FrozenDictionary<string, OutboundTarget> _targetsByName;

    /// <summary>
    /// Initializes a new instance of the <see cref="Topology" /> class from a compiled
    /// <see cref="TopologyData" /> snapshot.
    /// </summary>
    /// <param name="name">The name of the topology.</param>
    /// <param name="data">The compiled targets and endpoints that make up the topology.</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="name" /> is null or whitespace.</exception>
    protected Topology(string name, TopologyData data)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(name));
        }

        Name = name;
        _targetsByMessageType = data.TargetsByMessageType;
        _targetsByName = data.TargetsByName;
        _endpointsByName = data.EndpointsByName;
        OutboundTargets = data.OutboundTargets;
        InboundEndpoints = data.InboundEndpoints;
    }

    /// <summary>
    /// Gets the name of the topology.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Gets the outbound targets defined by the topology.
    /// </summary>
    public ImmutableArray<OutboundTarget> OutboundTargets { get; }

    /// <summary>
    /// Gets the inbound endpoints defined by the topology.
    /// </summary>
    public ImmutableArray<InboundEndpoint> InboundEndpoints { get; }

    /// <summary>
    /// Gets a value indicating whether the topology defines neither outbound targets nor inbound endpoints.
    /// </summary>
    public bool IsEmpty => OutboundTargets.IsDefaultOrEmpty && InboundEndpoints.IsDefaultOrEmpty;

    /// <summary>
    /// Gets a value indicating whether the topology defines outbound targets but no inbound endpoints.
    /// </summary>
    public bool IsOutboundOnly => !OutboundTargets.IsDefaultOrEmpty && InboundEndpoints.IsDefaultOrEmpty;

    /// <summary>
    /// Gets a value indicating whether the topology defines inbound endpoints but no outbound targets.
    /// </summary>
    public bool IsInboundOnly => OutboundTargets.IsDefaultOrEmpty && !InboundEndpoints.IsDefaultOrEmpty;

    /// <summary>
    /// Gets the outbound target registered for the given message type.
    /// </summary>
    /// <param name="messageType">The message type to look up.</param>
    /// <returns>The matching target.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="messageType" /> is <see langword="null" />.</exception>
    /// <exception cref="OutboundTargetNotFoundException">Thrown when no target is registered for <paramref name="messageType" />.</exception>
    public OutboundTarget GetRequiredTarget(Type messageType)
    {
        if (messageType is null)
        {
            throw new ArgumentNullException(nameof(messageType));
        }

        if (!_targetsByMessageType.TryGetValue(messageType, out var target))
        {
            throw new OutboundTargetNotFoundException(messageType);
        }

        return target;
    }

    /// <summary>
    /// Gets the strongly typed outbound target registered for message type <typeparamref name="T" />.
    /// </summary>
    /// <typeparam name="T">The message type to look up.</typeparam>
    /// <returns>The matching typed target.</returns>
    /// <exception cref="OutboundTargetNotFoundException">Thrown when no target is registered for <typeparamref name="T" />.</exception>
    /// <exception cref="OutboundTargetTypeMismatchException">Thrown when a target exists but is not typed for <typeparamref name="T" />.</exception>
    public OutboundTarget<T> GetRequiredTarget<T>()
    {
        var target = GetRequiredTarget(typeof(T));

        if (target is not OutboundTarget<T> typedTarget)
        {
            throw new OutboundTargetTypeMismatchException(target.Name, typeof(T), target.MessageType);
        }

        return typedTarget;
    }

    /// <summary>
    /// Gets the routable outbound target registered for message type <typeparamref name="T" />, used when the
    /// caller needs to supply a per-publish routing key.
    /// </summary>
    /// <typeparam name="T">The message type to look up.</typeparam>
    /// <returns>The matching routable target.</returns>
    /// <exception cref="OutboundTargetNotFoundException">Thrown when no target is registered for <typeparamref name="T" />.</exception>
    /// <exception cref="OutboundTargetTypeMismatchException">Thrown when a target exists but is not typed for <typeparamref name="T" />.</exception>
    /// <exception cref="OutboundTargetNotRoutableException">Thrown when the target does not support per-publish routing.</exception>
    public IOutboundRoutableTarget<T> GetRequiredRoutingTarget<T>()
    {
        var target = GetRequiredTarget<T>();

        if (target is not IOutboundRoutableTarget<T> routableTarget)
        {
            throw new OutboundTargetNotRoutableException(target.Name, typeof(T));
        }

        return routableTarget;
    }

    /// <summary>
    /// Gets the routable outbound target with the given name, typed for message type <typeparamref name="T" />.
    /// </summary>
    /// <typeparam name="T">The message type to look up.</typeparam>
    /// <param name="name">The name of the target.</param>
    /// <returns>The matching routable target.</returns>
    /// <exception cref="OutboundTargetNotFoundException">Thrown when no target with <paramref name="name" /> is registered.</exception>
    /// <exception cref="OutboundTargetTypeMismatchException">Thrown when the named target is not typed for <typeparamref name="T" />.</exception>
    /// <exception cref="OutboundTargetNotRoutableException">Thrown when the target does not support per-publish routing.</exception>
    public IOutboundRoutableTarget<T> GetRequiredRoutingTarget<T>(string name)
    {
        var target = GetRequiredTarget<T>(name);

        if (target is not IOutboundRoutableTarget<T> routableTarget)
        {
            throw new OutboundTargetNotRoutableException(target.Name, typeof(T));
        }

        return routableTarget;
    }

    /// <summary>
    /// Attempts to get the outbound target registered for the given message type without throwing when none
    /// exists.
    /// </summary>
    /// <param name="messageType">The message type to look up.</param>
    /// <param name="target">When this method returns, the matching target, or <see langword="null" /> when none was found.</param>
    /// <returns><see langword="true" /> when a target was found; otherwise <see langword="false" />.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="messageType" /> is <see langword="null" />.</exception>
    public bool TryGetTarget(Type messageType, out OutboundTarget? target)
    {
        if (messageType is null)
        {
            throw new ArgumentNullException(nameof(messageType));
        }

        return _targetsByMessageType.TryGetValue(messageType, out target);
    }

    /// <summary>
    /// Gets the outbound target registered under the given name.
    /// </summary>
    /// <param name="name">The name of the target.</param>
    /// <returns>The matching target.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="name" /> is null or whitespace.</exception>
    /// <exception cref="OutboundTargetNotFoundException">Thrown when no target with <paramref name="name" /> is registered.</exception>
    public OutboundTarget GetRequiredTarget(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(name));
        }

        if (!_targetsByName.TryGetValue(name, out var target))
        {
            throw new OutboundTargetNotFoundException(name);
        }

        return target;
    }

    /// <summary>
    /// Gets the outbound target registered under the given name, typed for message type <typeparamref name="T" />.
    /// </summary>
    /// <typeparam name="T">The expected message type of the target.</typeparam>
    /// <param name="name">The name of the target.</param>
    /// <returns>The matching typed target.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="name" /> is null or whitespace.</exception>
    /// <exception cref="OutboundTargetNotFoundException">Thrown when no target with <paramref name="name" /> is registered.</exception>
    /// <exception cref="OutboundTargetTypeMismatchException">Thrown when the named target is not typed for <typeparamref name="T" />.</exception>
    public OutboundTarget<T> GetRequiredTarget<T>(string name)
    {
        var target = GetRequiredTarget(name);

        if (target is not OutboundTarget<T> typedTarget)
        {
            throw new OutboundTargetTypeMismatchException(target.Name, typeof(T), target.MessageType);
        }

        return typedTarget;
    }

    /// <summary>
    /// Attempts to get the outbound target registered under the given name without throwing when none exists.
    /// </summary>
    /// <param name="name">The name of the target.</param>
    /// <param name="target">When this method returns, the matching target, or <see langword="null" /> when none was found.</param>
    /// <returns><see langword="true" /> when a target was found; otherwise <see langword="false" />.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="name" /> is null or whitespace.</exception>
    public bool TryGetTarget(string name, out OutboundTarget? target)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(name));
        }

        return _targetsByName.TryGetValue(name, out target);
    }

    /// <summary>
    /// Gets the inbound endpoint registered under the given name.
    /// </summary>
    /// <param name="name">The name of the endpoint.</param>
    /// <returns>The matching endpoint.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="name" /> is null or whitespace.</exception>
    /// <exception cref="InboundEndpointNotFoundException">Thrown when no endpoint with <paramref name="name" /> is registered.</exception>
    public InboundEndpoint GetRequiredEndpoint(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(name));
        }

        if (!_endpointsByName.TryGetValue(name, out var endpoint))
        {
            throw new InboundEndpointNotFoundException(name);
        }

        return endpoint;
    }

    /// <summary>
    /// Attempts to get the inbound endpoint registered under the given name without throwing when none exists.
    /// </summary>
    /// <param name="name">The name of the endpoint.</param>
    /// <param name="endpoint">When this method returns, the matching endpoint, or <see langword="null" /> when none was found.</param>
    /// <returns><see langword="true" /> when an endpoint was found; otherwise <see langword="false" />.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="name" /> is null or whitespace.</exception>
    public bool TryGetEndpoint(string name, out InboundEndpoint? endpoint)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(name));
        }

        return _endpointsByName.TryGetValue(name, out endpoint);
    }
}
