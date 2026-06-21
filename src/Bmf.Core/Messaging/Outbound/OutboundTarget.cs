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
    /// Gets the OpenTelemetry <c>messaging.system</c> value for publishes from this target. The base implementation
    /// returns <see cref="TransportName" /> (for the RabbitMQ transport this is already <c>rabbitmq</c>); transports
    /// override it only when the messaging-system identifier differs from the transport name.
    /// </summary>
    protected virtual string MessagingSystem => TransportName;

    /// <summary>
    /// Gets the OpenTelemetry <c>messaging.destination.name</c> for publishes from this target — the producer-side
    /// destination, known at activity-start time, that names the span and tags the metrics. The base returns
    /// <see langword="null" /> (no destination); a concrete transport target overrides it (for RabbitMQ, the
    /// exchange).
    /// </summary>
    protected virtual string? DestinationName => null;

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
        var diagnostics = StartPublishDiagnostics();
        try
        {
            diagnostics.SetMessage(message.MessageId, message.Body.Length);
            await PublishSerializedCoreAsync(message, cancellationToken).ConfigureAwait(false);
            diagnostics.Succeeded();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Only a cancellation of the caller's token is a graceful, non-error cancellation. An
            // OperationCanceledException raised while the caller's token is not signalled (an unrelated internal
            // timeout, say) is a genuine failure and falls through to the failure path below.
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
    /// Starts the publish diagnostics for a single publish: opens the <see cref="ActivityKind.Producer" /> activity,
    /// names it per the messaging span-name convention (<c>send {destination}</c>), and sets the transport-neutral
    /// <c>messaging.*</c> attributes known at start (<c>messaging.system</c>, <c>messaging.operation.type</c>,
    /// <c>messaging.operation.name</c>, and <c>messaging.destination.name</c>). The returned mutable struct is the
    /// single instrumented funnel for both the raw and typed publish paths; callers add the per-message attributes
    /// via <see cref="PublishDiagnostics.SetMessage" />, report the outcome, and then call
    /// <see cref="PublishDiagnostics.Record" />, which emits <c>messaging.client.sent.messages</c> and
    /// <c>messaging.client.operation.duration</c>. Returning a struct (rather than wrapping the work in a delegate)
    /// keeps the publish hot path free of the closure, delegate, and extra state-machine allocations a callback would
    /// introduce.
    /// </summary>
    private protected PublishDiagnostics StartPublishDiagnostics()
    {
        var system = MessagingSystem;
        var destination = DestinationName;

        var baseTags = new TagList
        {
            { MessagingSemanticConventions.MessagingSystem, system },
            { MessagingSemanticConventions.MessagingOperationName, MessagingSemanticConventions.SendOperation }
        };
        if (destination is not null)
        {
            baseTags.Add(MessagingSemanticConventions.MessagingDestinationName, destination);
        }

        var activity = OutboundDiagnostics.ActivitySource.StartActivity(
            PublishActivityName,
            ActivityKind.Producer
        );
        if (activity is not null)
        {
            activity.DisplayName = destination is null ?
                MessagingSemanticConventions.SendOperation :
                $"{MessagingSemanticConventions.SendOperation} {destination}";
            activity.SetTag(MessagingSemanticConventions.MessagingSystem, system);
            activity.SetTag(
                MessagingSemanticConventions.MessagingOperationType,
                MessagingSemanticConventions.SendOperation
            );
            activity.SetTag(
                MessagingSemanticConventions.MessagingOperationName,
                MessagingSemanticConventions.SendOperation
            );
            if (destination is not null)
            {
                activity.SetTag(MessagingSemanticConventions.MessagingDestinationName, destination);
            }
        }

        return new PublishDiagnostics(activity, baseTags, Stopwatch.GetTimestamp());
    }

    private static double GetDurationSeconds(long startedTimestamp)
    {
        var elapsedTicks = Stopwatch.GetTimestamp() - startedTimestamp;
        return (double) elapsedTicks / Stopwatch.Frequency;
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
        private string? _errorType;

        public PublishDiagnostics(Activity? activity, TagList baseTags, long startedTimestamp)
        {
            _activity = activity;
            _baseTags = baseTags;
            _startedTimestamp = startedTimestamp;
            _errorType = null;
        }

        /// <summary>
        /// Records the per-message <c>messaging.message.id</c> and <c>messaging.message.body.size</c> on the span.
        /// These are span-only attributes (the high-cardinality message id is never a metric dimension).
        /// </summary>
        public readonly void SetMessage(string? messageId, int bodySize)
        {
            if (_activity is null)
            {
                return;
            }

            if (messageId is not null)
            {
                _activity.SetTag(MessagingSemanticConventions.MessagingMessageId, messageId);
            }

            _activity.SetTag(MessagingSemanticConventions.MessagingMessageBodySize, bodySize);
        }

        public readonly void Succeeded()
        {
            _activity?.SetStatus(ActivityStatusCode.Ok);
        }

        public readonly void Cancelled()
        {
            // A graceful-shutdown cancellation is not an error: no error.type is set on the span or the metric.
        }

        public void Failed(Exception exception)
        {
            _errorType = MessagingSemanticConventions.ResolveErrorType(exception);
            _activity?.SetTag(MessagingSemanticConventions.ErrorType, _errorType);
            _activity?.SetStatus(ActivityStatusCode.Error);
            RecordException(_activity, exception);
        }

        public readonly void Record()
        {
            var outcomeTags = BuildOutcomeTags();
            OutboundDiagnostics.SentMessages.Add(1, outcomeTags);
            OutboundDiagnostics.OperationDuration.Record(GetDurationSeconds(_startedTimestamp), outcomeTags);
            _activity?.Dispose();
        }

        private readonly TagList BuildOutcomeTags()
        {
            var tags = _baseTags;
            if (_errorType is not null)
            {
                tags.Add(MessagingSemanticConventions.ErrorType, _errorType);
            }

            return tags;
        }

        private static void RecordException(Activity? activity, Exception exception)
        {
            if (activity is null)
            {
                return;
            }

            var tags = new ActivityTagsCollection
            {
                { "exception.type", exception.GetType().FullName },
                { "exception.message", exception.Message }
            };

            if (exception.StackTrace is not null)
            {
                tags.Add("exception.stacktrace", exception.StackTrace);
            }

            activity.AddEvent(new ActivityEvent("exception", tags: tags));
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
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="serializer" /> or <paramref name="messageContractRegistry" /> is
    /// <see langword="null" />.
    /// </exception>
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
        var diagnostics = StartPublishDiagnostics();
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

            diagnostics.SetMessage(envelope.Id, envelope.Data.Length);
            await PublishTypedCloudEventAsync(message, envelope, routingKey, cancellationToken).ConfigureAwait(false);
            diagnostics.Succeeded();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Only a cancellation of the caller's token is a graceful, non-error cancellation. An
            // OperationCanceledException raised while the caller's token is not signalled (an unrelated internal
            // timeout, say) is a genuine failure and falls through to the failure path below.
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
