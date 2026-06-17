using System;
using System.Threading;
using System.Threading.Tasks;
using Bmf.Abstractions;

namespace Bmf.Core.Messaging.Outbound;

/// <summary>
/// Serializes messages as CloudEvents v1.0 envelopes in binary content mode.
/// </summary>
/// <remarks>
/// The serializer assembles transport-neutral attributes. A transport applies its protocol binding. The
/// serializer never generates retry-sensitive id or time attributes.
/// </remarks>
public sealed class CloudEventMessageSerializer : IMessageSerializer
{
    private readonly CloudEventsOptions _options;
    private readonly IPayloadCodec _payloadCodec;

    /// <summary>
    /// Initializes a new instance of the <see cref="CloudEventMessageSerializer" /> class.
    /// </summary>
    /// <param name="payloadCodec">The codec used to encode the message body.</param>
    /// <param name="options">The CloudEvents options supplying the default source.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="payloadCodec" /> or <paramref name="options" /> is <see langword="null" />.</exception>
    public CloudEventMessageSerializer(
        IPayloadCodec payloadCodec,
        CloudEventsOptions options
    )
    {
        _payloadCodec = payloadCodec ?? throw new ArgumentNullException(nameof(payloadCodec));
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <inheritdoc />
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="message" /> is <see langword="null" />.</exception>
    /// <exception cref="CloudEventMetadataException">Thrown when a required CloudEvents attribute (id, time, source, type, or data content type) is missing or invalid.</exception>
    public ValueTask<CloudEventEnvelope> SerializeAsync<T>(
        T message,
        in CloudEventMetadata metadata,
        string? type,
        string? dataSchema,
        CancellationToken cancellationToken = default
    )
    {
        if (message is null)
        {
            throw new ArgumentNullException(nameof(message));
        }

        if (metadata.Id == Guid.Empty)
        {
            throw new CloudEventMetadataException(
                CloudEventAttributeNames.Id,
                "Implement ICloudEvent or derive from BaseCloudEvent, or pass CloudEventMetadata with a non-empty Id."
            );
        }

        if (metadata.Time == default)
        {
            throw new CloudEventMetadataException(
                CloudEventAttributeNames.Time,
                "Implement ICloudEvent or derive from BaseCloudEvent, or pass CloudEventMetadata with a construction-time Time value."
            );
        }

        var source = CloudEventsOptionsValidation.GetRequiredSource(metadata.Source ?? _options.Source);
        var resolvedType = GetRequiredType(type);
        var payload = _payloadCodec.Encode(message);

        if (string.IsNullOrWhiteSpace(payload.DataContentType))
        {
            throw new CloudEventMetadataException(
                CloudEventAttributeNames.DataContentType,
                "Configure the payload codec to return a non-empty data content type."
            );
        }

        CloudEventEnvelope envelope = new (
            "1.0",
            metadata.Id.ToString("D"),
            source,
            resolvedType,
            metadata.Time,
            metadata.Subject,
            payload.DataContentType,
            dataSchema,
            payload.Data
        );

        return new ValueTask<CloudEventEnvelope>(envelope);
    }

    private static string GetRequiredType(string? type)
    {
        if (string.IsNullOrWhiteSpace(type))
        {
            throw new CloudEventMetadataException(
                CloudEventAttributeNames.Type,
                "Resolve a non-empty CloudEvents type discriminator before serializing the message."
            );
        }

        return type!;
    }
}
