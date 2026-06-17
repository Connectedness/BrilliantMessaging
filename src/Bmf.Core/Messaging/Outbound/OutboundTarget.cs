using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Bmf.Abstractions;

namespace Bmf.Core.Messaging.Outbound;

/// <summary>
/// Represents a destination a message is published to (for example a RabbitMQ exchange). It is the
/// non-generic base of the outbound extension model and owns the cross-cutting publish concerns — the
/// diagnostics activity, the attempt/failure counters, and the duration measurement — so that every
/// concrete target is instrumented uniformly.
/// </summary>
/// <remarks>
/// Transport authors do not derive from this type directly; they derive from <see cref="OutboundTarget{T}" />,
/// which adds serialization and contract resolution. The non-generic base exists so that components which
/// only handle already-serialized payloads (such as <see cref="MessagePublisher" />) can treat targets
/// uniformly through <see cref="PublishSerializedAsync" />.
/// </remarks>
public abstract class OutboundTarget
{
    private const string SerializedMessageTypeName = "serialized";

    private const string PublishActivityName = "bmf.outbound.publish";

    /// <summary>
    /// Initializes a new instance of the <see cref="OutboundTarget" /> class.
    /// </summary>
    /// <param name="name">The logical name of the target, used to look it up and to tag diagnostics.</param>
    /// <param name="transportName">The name of the transport that backs the target (for example the RabbitMQ transport).</param>
    /// <param name="topologyName">
    /// The name of the topology the target belongs to, or <see langword="null" /> to use
    /// <see cref="Topology.DefaultName" />.
    /// </param>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="name" /> or <paramref name="transportName" /> is null or whitespace, or when
    /// <paramref name="topologyName" /> is non-null but whitespace.
    /// </exception>
    protected OutboundTarget(string name, string transportName, string? topologyName = null)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(name));
        }

        if (string.IsNullOrWhiteSpace(transportName))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(transportName));
        }

        if (topologyName is not null && string.IsNullOrWhiteSpace(topologyName))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(topologyName));
        }

        Name = name;
        TransportName = transportName;
        TopologyName = topologyName ?? Topology.DefaultName;
    }

    /// <summary>
    /// Gets the message type the target publishes, or <see langword="null" /> for a target that only handles
    /// already-serialized payloads. Overridden by <see cref="OutboundTarget{T}" /> to return the typed contract.
    /// </summary>
    public virtual Type? MessageType => null;

    /// <summary>
    /// Gets the logical name of the target.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Gets the name of the topology the target belongs to.
    /// </summary>
    public string TopologyName { get; }

    /// <summary>
    /// Gets the name of the transport that backs the target.
    /// </summary>
    public string TransportName { get; }

    /// <summary>
    /// Resolves the value used to tag the runtime message type in publish diagnostics. The base implementation
    /// returns the type's full name; <see cref="OutboundTarget{T}" /> overrides it to return the registered
    /// message-contract discriminator.
    /// </summary>
    /// <param name="runtimeMessageType">The runtime type of the message being published.</param>
    /// <returns>The diagnostic name for <paramref name="runtimeMessageType" />.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="runtimeMessageType" /> is <see langword="null" />.</exception>
    public virtual string GetDiagnosticMessageTypeName(Type runtimeMessageType)
    {
        if (runtimeMessageType is null)
        {
            throw new ArgumentNullException(nameof(runtimeMessageType));
        }

        return runtimeMessageType.FullName ?? runtimeMessageType.Name;
    }

    /// <summary>
    /// Publishes an already serialized message. This is a non-virtual template that owns the publish
    /// diagnostics (activity, attempt/failure counters, and duration) and delegates the transport-specific
    /// dispatch to <see cref="PublishSerializedCoreAsync" />, so raw publishes are always instrumented by the
    /// base target layer regardless of how callers reach the target.
    /// </summary>
    public async Task PublishSerializedAsync(
        SerializedMessage message,
        CancellationToken cancellationToken = default
    )
    {
        var diagnostics = StartPublishDiagnostics(GetRawDiagnosticMessageTypeName());
        try
        {
            await PublishSerializedCoreAsync(message, cancellationToken).ConfigureAwait(false);
            diagnostics.Succeeded();
        }
        catch (OperationCanceledException)
        {
            diagnostics.Cancelled();
            throw;
        }
        catch (Exception exception)
        {
            diagnostics.Failed(exception);
            throw;
        }
        finally
        {
            diagnostics.Record();
        }
    }

    /// <summary>
    /// Performs the transport-specific dispatch of an already serialized message. Implementers do the wire
    /// publish only; the base class has already opened the diagnostics scope around this call, so an overrider
    /// must not add publish counters or activities of its own.
    /// </summary>
    /// <param name="message">The serialized message to dispatch.</param>
    /// <param name="cancellationToken">A token to observe while waiting for the dispatch to complete.</param>
    /// <returns>A task that completes when the transport has accepted the message.</returns>
    protected abstract Task PublishSerializedCoreAsync(
        SerializedMessage message,
        CancellationToken cancellationToken
    );

    /// <summary>
    /// Starts the publish diagnostics for a single attempt: opens the activity, sets the common tags, and
    /// records the attempt counter. The returned mutable struct is the single instrumented funnel for both the
    /// raw and typed publish paths; callers report the outcome and then call <see cref="PublishDiagnostics.Record" />.
    /// Returning a struct (rather than wrapping the work in a delegate) keeps the publish hot path free of the
    /// closure, delegate, and extra state-machine allocations a callback would introduce.
    /// </summary>
    private protected PublishDiagnostics StartPublishDiagnostics(string messageTypeName)
    {
        var baseTags = new TagList
        {
            { OutboundDiagnostics.MessageTypeTagName, messageTypeName },
            { OutboundDiagnostics.TargetNameTagName, Name },
            { OutboundDiagnostics.TransportNameTagName, TransportName }
        };

        var activity = OutboundDiagnostics.ActivitySource.StartActivity(
            PublishActivityName,
            ActivityKind.Producer
        );
        if (activity is not null)
        {
            activity.SetTag(OutboundDiagnostics.MessageTypeTagName, messageTypeName);
            activity.SetTag(OutboundDiagnostics.TargetNameTagName, Name);
            activity.SetTag(OutboundDiagnostics.TransportNameTagName, TransportName);
        }

        var startedTimestamp = Stopwatch.GetTimestamp();
        OutboundDiagnostics.PublishAttempts.Add(1, baseTags);

        return new PublishDiagnostics(activity, baseTags, startedTimestamp);
    }

    private string GetRawDiagnosticMessageTypeName()
    {
        // Every concrete target today derives from OutboundTarget<T>, so MessageType is non-null and the
        // raw path tags the typed discriminator. The "serialized" fallback exists only for a non-generic
        // OutboundTarget; it is unreachable until such a target is introduced.
        if (MessageType is null)
        {
            return SerializedMessageTypeName;
        }

        return GetDiagnosticMessageTypeName(MessageType);
    }

    private static double GetDurationMilliseconds(long startedTimestamp)
    {
        var elapsedTicks = Stopwatch.GetTimestamp() - startedTimestamp;
        return elapsedTicks * 1000d / Stopwatch.Frequency;
    }

    private static string GetDeliveryFailureReasonName(MessageDeliveryFailureReason reason)
    {
        return reason switch
        {
            MessageDeliveryFailureReason.Nacked => "nacked",
            MessageDeliveryFailureReason.Returned => "returned",
            MessageDeliveryFailureReason.Timeout => "timeout",
            _ => throw new ArgumentOutOfRangeException(nameof(reason), reason, "Unsupported delivery-failure reason.")
        };
    }

    /// <summary>
    /// Carries the per-publish diagnostics state across the publish operation. It is a mutable struct on
    /// purpose: living as a local of the caller's async method, it avoids the heap allocations a delegate-based
    /// instrumentation wrapper would incur, while the base tag set is held in a stack-resident
    /// <see cref="TagList" /> rather than re-allocated as an array for every measurement.
    /// </summary>
    private protected struct PublishDiagnostics
    {
        private readonly Activity? _activity;
        private readonly TagList _baseTags;
        private readonly long _startedTimestamp;
        private string _outcome;
        private string? _deliveryFailureReason;

        public PublishDiagnostics(Activity? activity, TagList baseTags, long startedTimestamp)
        {
            _activity = activity;
            _baseTags = baseTags;
            _startedTimestamp = startedTimestamp;
            _outcome = "success";
            _deliveryFailureReason = null;
        }

        public readonly void Succeeded()
        {
            _activity?.SetStatus(ActivityStatusCode.Ok);
        }

        public void Cancelled()
        {
            _outcome = "cancelled";
        }

        public void Failed(Exception exception)
        {
            _outcome = "failure";
            _deliveryFailureReason = exception is MessageDeliveryException deliveryException ?
                GetDeliveryFailureReasonName(deliveryException.Reason) :
                null;

            OutboundDiagnostics.PublishFailures.Add(1, BuildOutcomeTags());
            _activity?.SetStatus(ActivityStatusCode.Error);
            if (_deliveryFailureReason is not null)
            {
                _activity?.SetTag(OutboundDiagnostics.DeliveryFailureReasonTagName, _deliveryFailureReason);
            }
        }

        public readonly void Record()
        {
            OutboundDiagnostics.PublishDuration.Record(GetDurationMilliseconds(_startedTimestamp), BuildOutcomeTags());
            _activity?.SetTag(OutboundDiagnostics.OutcomeTagName, _outcome);
            _activity?.Dispose();
        }

        private readonly TagList BuildOutcomeTags()
        {
            var tags = _baseTags;
            tags.Add(OutboundDiagnostics.OutcomeTagName, _outcome);
            if (_deliveryFailureReason is not null)
            {
                tags.Add(OutboundDiagnostics.DeliveryFailureReasonTagName, _deliveryFailureReason);
            }

            return tags;
        }
    }
}

/// <summary>
/// Represents a strongly typed outbound target for messages of type <typeparamref name="T" />. This is the
/// base class transport authors derive from: it owns serialization, CloudEvents metadata resolution, and the
/// instrumented publish template, leaving the subclass to implement only the wire-level dispatch.
/// </summary>
/// <remarks>
/// The publish flow is a template method. <see cref="PublishAsync(T, CancellationToken)" /> and its overloads
/// funnel into <see cref="PublishCoreAsync" />, which resolves the contract discriminator and data schema,
/// serializes the message into a <see cref="CloudEventEnvelope" /> via <see cref="Serializer" />, and then calls
/// the subclass-supplied <see cref="PublishTypedCloudEventAsync" /> for the actual transport dispatch. Metadata
/// resolution deliberately runs outside the instrumented region so that contract-registration mistakes are not
/// reported as publish failures.
/// </remarks>
/// <typeparam name="T">The message type this target publishes.</typeparam>
public abstract class OutboundTarget<T> : OutboundTarget
{
    /// <summary>
    /// Initializes a new instance of the <see cref="OutboundTarget{T}" /> class.
    /// </summary>
    /// <param name="name">The logical name of the target.</param>
    /// <param name="transportName">The name of the transport that backs the target.</param>
    /// <param name="serializer">The serializer used to turn messages into <see cref="CloudEventEnvelope" /> instances.</param>
    /// <param name="messageContractRegistry">The registry used to resolve the contract discriminator and data schema for a message type.</param>
    /// <param name="topologyName">The name of the topology the target belongs to, or <see langword="null" /> for the default topology.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="serializer" /> or <paramref name="messageContractRegistry" /> is <see langword="null" />.</exception>
    protected OutboundTarget(
        string name,
        string transportName,
        IMessageSerializer serializer,
        IMessageContractRegistry messageContractRegistry,
        string? topologyName = null
    )
        : base(name, transportName, topologyName)
    {
        Serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        MessageContractRegistry = messageContractRegistry ??
                                  throw new ArgumentNullException(nameof(messageContractRegistry));
    }

    /// <inheritdoc />
    public sealed override Type MessageType => typeof(T);

    /// <summary>
    /// Gets the serializer the publish template uses to turn a message into a <see cref="CloudEventEnvelope" />.
    /// Available to subclasses that need to serialize outside the standard template.
    /// </summary>
    protected IMessageSerializer Serializer { get; }

    /// <summary>
    /// Gets the message-contract registry the target uses to resolve discriminators and data schemas.
    /// </summary>
    protected IMessageContractRegistry MessageContractRegistry { get; }

    /// <inheritdoc />
    public sealed override string GetDiagnosticMessageTypeName(Type runtimeMessageType)
    {
        return GetRequiredDiscriminator(runtimeMessageType);
    }

    /// <summary>
    /// Resolves the registered contract discriminator (the CloudEvents <c>type</c> attribute) for the given
    /// runtime message type.
    /// </summary>
    /// <param name="runtimeMessageType">The runtime type of the message.</param>
    /// <returns>The discriminator the message type was registered with.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="runtimeMessageType" /> is <see langword="null" />.</exception>
    /// <exception cref="CloudEventMetadataException">
    /// Thrown when <paramref name="runtimeMessageType" /> has no registered contract; the message explains how to
    /// register it with the <see cref="MessageContractRegistryBuilder" />.
    /// </exception>
    public string GetRequiredDiscriminator(Type runtimeMessageType)
    {
        if (runtimeMessageType is null)
        {
            throw new ArgumentNullException(nameof(runtimeMessageType));
        }

        try
        {
            return MessageContractRegistry.GetDiscriminator(runtimeMessageType);
        }
        catch (MessageContractNotRegisteredException exception)
        {
            throw new CloudEventMetadataException(
                CloudEventAttributeNames.Type,
                $"Register the runtime message type '{exception.MessageType}' with MessageContractRegistryBuilder.Map<T>(...) or MapOutbound<T>(...)."
            );
        }
    }

    /// <summary>
    /// Resolves the optional data schema (the CloudEvents <c>dataschema</c> attribute) registered for the given
    /// runtime message type.
    /// </summary>
    /// <param name="runtimeMessageType">The runtime type of the message.</param>
    /// <returns>The registered data schema, or <see langword="null" /> when none was registered.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="runtimeMessageType" /> is <see langword="null" />.</exception>
    public string? GetDataSchema(Type runtimeMessageType)
    {
        if (runtimeMessageType is null)
        {
            throw new ArgumentNullException(nameof(runtimeMessageType));
        }

        return MessageContractRegistry.GetDataSchema(runtimeMessageType);
    }

    /// <summary>
    /// Publishes a message, deriving the CloudEvents metadata from the message itself. The message must implement
    /// <see cref="ICloudEvent" /> (typically by deriving from <see cref="BaseCloudEvent" />); otherwise use an
    /// overload that takes an explicit <see cref="CloudEventMetadata" />.
    /// </summary>
    /// <param name="message">The message to publish.</param>
    /// <param name="cancellationToken">A token to observe while waiting for the publish to complete.</param>
    /// <returns>A task that completes when the transport has accepted the message.</returns>
    /// <exception cref="CloudEventMetadataException">Thrown when <paramref name="message" /> does not implement <see cref="ICloudEvent" />.</exception>
    public Task PublishAsync(
        T message,
        CancellationToken cancellationToken = default
    )
    {
        if (message is not ICloudEvent cloudEvent)
        {
            throw new CloudEventMetadataException(
                CloudEventAttributeNames.Id,
                "Implement ICloudEvent or derive from BaseCloudEvent, or call PublishAsync with explicit CloudEventMetadata."
            );
        }

        var metadata = CloudEventMetadata.From(cloudEvent);
        return PublishAsync(message, in metadata, cancellationToken);
    }

    /// <summary>
    /// Publishes a message with explicit CloudEvents metadata, resolving the <c>type</c> and <c>dataschema</c>
    /// attributes from the message contract registry.
    /// </summary>
    /// <param name="message">The message to publish.</param>
    /// <param name="metadata">The CloudEvents metadata (id, source, time, subject) to attach.</param>
    /// <param name="cancellationToken">A token to observe while waiting for the publish to complete.</param>
    /// <returns>A task that completes when the transport has accepted the message.</returns>
    public Task PublishAsync(
        T message,
        in CloudEventMetadata metadata,
        CancellationToken cancellationToken = default
    )
    {
        return PublishCoreAsync(message, metadata, type: null, dataSchema: null, routingKey: null, cancellationToken);
    }

    /// <summary>
    /// Publishes a message with explicit CloudEvents metadata and an explicit <c>type</c> and <c>dataschema</c>,
    /// bypassing contract-registry resolution of those attributes.
    /// </summary>
    /// <param name="message">The message to publish.</param>
    /// <param name="metadata">The CloudEvents metadata (id, source, time, subject) to attach.</param>
    /// <param name="type">The CloudEvents <c>type</c> attribute to use.</param>
    /// <param name="dataSchema">The CloudEvents <c>dataschema</c> attribute to use, or <see langword="null" /> for none.</param>
    /// <param name="cancellationToken">A token to observe while waiting for the publish to complete.</param>
    /// <returns>A task that completes when the transport has accepted the message.</returns>
    public Task PublishAsync(
        T message,
        in CloudEventMetadata metadata,
        string type,
        string? dataSchema,
        CancellationToken cancellationToken = default
    )
    {
        return PublishCoreAsync(message, metadata, type, dataSchema, routingKey: null, cancellationToken);
    }

    /// <summary>
    /// Runs the instrumented publish template: resolves any unspecified <c>type</c>/<c>dataschema</c>, serializes
    /// the message into a <see cref="CloudEventEnvelope" />, and dispatches it through
    /// <see cref="PublishTypedCloudEventAsync" />. Subclasses call this from custom publish entry points (for
    /// example to supply a transport-specific <paramref name="routingKey" />) so that the diagnostics and
    /// serialization behaviour stay consistent.
    /// </summary>
    /// <param name="message">The message to publish.</param>
    /// <param name="metadata">The CloudEvents metadata to attach.</param>
    /// <param name="type">The CloudEvents <c>type</c>, or <see langword="null" /> to resolve it from the contract registry.</param>
    /// <param name="dataSchema">The CloudEvents <c>dataschema</c>, or <see langword="null" /> to resolve it from the contract registry.</param>
    /// <param name="routingKey">An optional transport routing key passed through to <see cref="PublishTypedCloudEventAsync" />.</param>
    /// <param name="cancellationToken">A token to observe while waiting for the publish to complete.</param>
    /// <returns>A task that completes when the transport has accepted the message.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="message" /> is <see langword="null" />.</exception>
    /// <exception cref="MessageSerializationException">Thrown when serializing the message fails.</exception>
    protected async Task PublishCoreAsync(
        T message,
        CloudEventMetadata metadata,
        string? type,
        string? dataSchema,
        string? routingKey,
        CancellationToken cancellationToken
    )
    {
        if (message is null)
        {
            throw new ArgumentNullException(nameof(message));
        }

        var runtimeType = message.GetType();

        // Discriminator and data-schema resolution happen before the instrumented region so that
        // metadata-resolution failures are never counted as publish failures, and so the direct path
        // (type resolved here) and the publisher-mediated path (type resolved upstream) behave identically.
        var resolvedType = type ?? GetRequiredDiscriminator(runtimeType);
        var resolvedDataSchema = dataSchema ?? GetDataSchema(runtimeType);

        // The instrumented region begins here, at serialization, and runs through transport dispatch.
        var diagnostics = StartPublishDiagnostics(resolvedType);
        try
        {
            CloudEventEnvelope envelope;
            try
            {
                envelope = await Serializer.SerializeAsync(
                        message,
                        in metadata,
                        resolvedType,
                        resolvedDataSchema,
                        cancellationToken
                    )
                   .ConfigureAwait(false);
            }
            catch (Exception exception) when (exception is not OperationCanceledException &&
                                              exception is not MessageSerializationException)
            {
                throw new MessageSerializationException(runtimeType, exception);
            }

            await PublishTypedCloudEventAsync(message, envelope, routingKey, cancellationToken).ConfigureAwait(false);
            diagnostics.Succeeded();
        }
        catch (OperationCanceledException)
        {
            diagnostics.Cancelled();
            throw;
        }
        catch (Exception exception)
        {
            diagnostics.Failed(exception);
            throw;
        }
        finally
        {
            diagnostics.Record();
        }
    }

    /// <summary>
    /// Performs the transport-specific dispatch of a serialized typed CloudEvent. This is the single method a
    /// concrete target must implement; the base class has already serialized the message and opened the
    /// diagnostics scope, so the override only does the wire publish and must not add its own publish counters or
    /// activities.
    /// </summary>
    /// <param name="message">The original message, available for transport routing decisions.</param>
    /// <param name="envelope">The serialized CloudEvent to dispatch.</param>
    /// <param name="routingKey">The optional routing key supplied to <see cref="PublishCoreAsync" />, or <see langword="null" />.</param>
    /// <param name="cancellationToken">A token to observe while waiting for the dispatch to complete.</param>
    /// <returns>A task that completes when the transport has accepted the message.</returns>
    protected abstract Task PublishTypedCloudEventAsync(
        T message,
        CloudEventEnvelope envelope,
        string? routingKey,
        CancellationToken cancellationToken
    );
}
