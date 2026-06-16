using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Usf.Abstractions;
using Usf.Core.Messaging.Errors;

namespace Usf.Core.Messaging;

public abstract class OutboundTarget
{
    private const string SerializedMessageTypeName = "serialized";

    private const string PublishActivityName = "usf.outbound.publish";

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

    public virtual Type? MessageType => null;

    public string Name { get; }

    public string TopologyName { get; }

    public string TransportName { get; }

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

public abstract class OutboundTarget<T> : OutboundTarget
{
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

    public sealed override Type MessageType => typeof(T);

    protected IMessageSerializer Serializer { get; }

    protected IMessageContractRegistry MessageContractRegistry { get; }

    public sealed override string GetDiagnosticMessageTypeName(Type runtimeMessageType)
    {
        return GetRequiredDiscriminator(runtimeMessageType);
    }

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

    public string? GetDataSchema(Type runtimeMessageType)
    {
        if (runtimeMessageType is null)
        {
            throw new ArgumentNullException(nameof(runtimeMessageType));
        }

        return MessageContractRegistry.GetDataSchema(runtimeMessageType);
    }

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

    public Task PublishAsync(
        T message,
        in CloudEventMetadata metadata,
        CancellationToken cancellationToken = default
    )
    {
        return PublishCoreAsync(message, metadata, type: null, dataSchema: null, routingKey: null, cancellationToken);
    }

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

    protected abstract Task PublishTypedCloudEventAsync(
        T message,
        CloudEventEnvelope envelope,
        string? routingKey,
        CancellationToken cancellationToken
    );
}
