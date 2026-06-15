using System;
using System.Collections.Generic;
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
    public Task PublishSerializedAsync(
        SerializedMessage message,
        CancellationToken cancellationToken = default
    )
    {
        return PublishWithDiagnosticsAsync(
            GetRawDiagnosticMessageTypeName(),
            () => PublishSerializedCoreAsync(message, cancellationToken)
        );
    }

    protected abstract Task PublishSerializedCoreAsync(
        SerializedMessage message,
        CancellationToken cancellationToken
    );

    private protected async Task PublishWithDiagnosticsAsync(string messageTypeName, Func<Task> publishAsync)
    {
        var tags = CreateBaseTags(messageTypeName, Name, TransportName);
        var activity = OutboundDiagnostics.ActivitySource.StartActivity(
            PublishActivityName,
            ActivityKind.Producer
        );
        var startedTimestamp = Stopwatch.GetTimestamp();

        SetCommonTags(activity, messageTypeName, Name, TransportName);
        OutboundDiagnostics.PublishAttempts.Add(1, tags);

        var outcome = "success";
        string? deliveryFailureReason = null;

        try
        {
            await publishAsync().ConfigureAwait(false);
            activity?.SetStatus(ActivityStatusCode.Ok);
        }
        catch (OperationCanceledException)
        {
            outcome = "cancelled";
            activity?.SetTag(OutboundDiagnostics.OutcomeTagName, outcome);
            throw;
        }
        catch (Exception exception)
        {
            outcome = "failure";
            deliveryFailureReason = exception is MessageDeliveryException deliveryException ?
                GetDeliveryFailureReasonName(deliveryException.Reason) :
                null;
            OutboundDiagnostics.PublishFailures.Add(
                1,
                CreateBaseTags(
                    messageTypeName,
                    Name,
                    TransportName,
                    outcome,
                    deliveryFailureReason
                )
            );
            activity?.SetStatus(ActivityStatusCode.Error);
            activity?.SetTag(OutboundDiagnostics.OutcomeTagName, outcome);
            activity?.SetTag(OutboundDiagnostics.DeliveryFailureReasonTagName, deliveryFailureReason);
            throw;
        }
        finally
        {
            var durationMilliseconds = GetDurationMilliseconds(startedTimestamp);
            var durationTags = CreateBaseTags(
                messageTypeName,
                Name,
                TransportName,
                outcome,
                deliveryFailureReason
            );
            OutboundDiagnostics.PublishDuration.Record(durationMilliseconds, durationTags);
            activity?.SetTag(OutboundDiagnostics.OutcomeTagName, outcome);
            activity?.Dispose();
        }
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

    private static KeyValuePair<string, object?>[] CreateBaseTags(
        string messageTypeName,
        string targetName,
        string transportName,
        string? outcome = null,
        string? deliveryFailureReason = null
    )
    {
        if (outcome is null)
        {
            return
            [
                new KeyValuePair<string, object?>(OutboundDiagnostics.MessageTypeTagName, messageTypeName),
                new KeyValuePair<string, object?>(OutboundDiagnostics.TargetNameTagName, targetName),
                new KeyValuePair<string, object?>(OutboundDiagnostics.TransportNameTagName, transportName)
            ];
        }

        if (deliveryFailureReason is null)
        {
            return
            [
                new KeyValuePair<string, object?>(OutboundDiagnostics.MessageTypeTagName, messageTypeName),
                new KeyValuePair<string, object?>(OutboundDiagnostics.TargetNameTagName, targetName),
                new KeyValuePair<string, object?>(OutboundDiagnostics.TransportNameTagName, transportName),
                new KeyValuePair<string, object?>(OutboundDiagnostics.OutcomeTagName, outcome)
            ];
        }

        return
        [
            new KeyValuePair<string, object?>(OutboundDiagnostics.MessageTypeTagName, messageTypeName),
            new KeyValuePair<string, object?>(OutboundDiagnostics.TargetNameTagName, targetName),
            new KeyValuePair<string, object?>(OutboundDiagnostics.TransportNameTagName, transportName),
            new KeyValuePair<string, object?>(OutboundDiagnostics.OutcomeTagName, outcome),
            new KeyValuePair<string, object?>(
                OutboundDiagnostics.DeliveryFailureReasonTagName,
                deliveryFailureReason
            )
        ];
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

    private static void SetCommonTags(
        Activity? activity,
        string messageTypeName,
        string targetName,
        string transportName
    )
    {
        activity?.SetTag(OutboundDiagnostics.MessageTypeTagName, messageTypeName);
        activity?.SetTag(OutboundDiagnostics.TargetNameTagName, targetName);
        activity?.SetTag(OutboundDiagnostics.TransportNameTagName, transportName);
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

        await PublishWithDiagnosticsAsync(
                resolvedType,
                async () =>
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

                    await PublishTypedCloudEventAsync(message, envelope, routingKey, cancellationToken)
                       .ConfigureAwait(false);
                }
            )
           .ConfigureAwait(false);
    }

    protected abstract Task PublishTypedCloudEventAsync(
        T message,
        CloudEventEnvelope envelope,
        string? routingKey,
        CancellationToken cancellationToken
    );
}
