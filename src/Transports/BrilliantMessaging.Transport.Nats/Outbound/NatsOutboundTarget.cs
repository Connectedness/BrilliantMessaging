using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Abstractions;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;
using BrilliantMessaging.Core.Messaging.Outbound;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace BrilliantMessaging.Transport.Nats.Outbound;

/// <summary>
/// NATS JetStream outbound target. Publishes CloudEvents binary content mode over NATS headers and awaits the
/// JetStream publish acknowledgement.
/// </summary>
public sealed class NatsOutboundTarget<TMessage> : OutboundTarget<TMessage>
{
    /// <summary>
    /// The CloudEvents binary content-mode header prefix used on the NATS wire.
    /// </summary>
    public const string CloudEventsWireHeaderPrefix = "ce-";

    private readonly NatsConnectionProvider _connectionProvider;

    private readonly bool _messageIdDeduplication;
    private readonly string _subject;

    /// <summary>
    /// Initializes a new instance of the <see cref="NatsOutboundTarget{TMessage}" /> class.
    /// </summary>
    public NatsOutboundTarget(
        string name,
        IMessageSerializer serializer,
        IMessageContractRegistry messageContractRegistry,
        string topologyName,
        string subject,
        bool messageIdDeduplication,
        NatsConnectionProvider connectionProvider
    ) : base(name, NatsTopology.TransportNameValue, serializer, messageContractRegistry, topologyName)
    {
        if (string.IsNullOrWhiteSpace(subject))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(subject));
        }

        _subject = subject;
        _messageIdDeduplication = messageIdDeduplication;
        _connectionProvider = connectionProvider ?? throw new ArgumentNullException(nameof(connectionProvider));
        DestinationName = subject;
    }

    /// <inheritdoc />
    protected override string? DestinationName { get; }

    /// <inheritdoc />
    protected override async Task PublishSerializedCoreAsync(
        SerializedMessage message,
        CancellationToken cancellationToken
    )
    {
        cancellationToken.ThrowIfCancellationRequested();

        var headers = CreateHeaders(message.Headers, message.ContentType, message.ContentEncoding, message.MessageId);
        var options = CreatePublishOptions(message.MessageId);
        var jetStream = await _connectionProvider.GetJetStreamAsync(cancellationToken).ConfigureAwait(false);
        var acknowledgement = await jetStream
           .PublishAsync(
                _subject,
                message.Body,
                serializer: null,
                options,
                headers,
                cancellationToken
            )
           .ConfigureAwait(false);
        EnsureAccepted(acknowledgement);
    }

    /// <inheritdoc />
    protected override async Task PublishTypedCloudEventAsync(
        TMessage message,
        CloudEventEnvelope envelope,
        string? routingKey,
        CancellationToken cancellationToken
    )
    {
        cancellationToken.ThrowIfCancellationRequested();

        var headers = CreateCloudEventHeaders(envelope);
        var options = CreatePublishOptions(envelope.Id);
        var jetStream = await _connectionProvider.GetJetStreamAsync(cancellationToken).ConfigureAwait(false);
        var acknowledgement = await jetStream
           .PublishAsync(
                _subject,
                envelope.Data,
                serializer: null,
                options,
                headers,
                cancellationToken
            )
           .ConfigureAwait(false);
        EnsureAccepted(acknowledgement);
    }

    private void EnsureAccepted(PubAckResponse acknowledgement)
    {
        if (_messageIdDeduplication && acknowledgement is { Error: null, Duplicate: true })
        {
            return;
        }

        acknowledgement.EnsureSuccess();
    }

    private NatsJSPubOpts? CreatePublishOptions(string? messageId)
    {
        if (!_messageIdDeduplication || string.IsNullOrWhiteSpace(messageId))
        {
            return null;
        }

        return new NatsJSPubOpts { MsgId = messageId };
    }

    private static NatsHeaders CreateHeaders(
        IReadOnlyDictionary<string, string?> headers,
        string? contentType,
        string? contentEncoding,
        string? messageId
    )
    {
        NatsHeaders natsHeaders = new (headers.Count + 4);
        foreach (var header in headers)
        {
            natsHeaders[MapHeaderName(header.Key)] = header.Value;
        }

        if (contentType is not null)
        {
            natsHeaders["content-type"] = contentType;
        }

        if (contentEncoding is not null)
        {
            natsHeaders["content-encoding"] = contentEncoding;
        }

        if (messageId is not null)
        {
            natsHeaders["message-id"] = messageId;
        }

        return natsHeaders;
    }

    private static NatsHeaders CreateCloudEventHeaders(CloudEventEnvelope envelope)
    {
        var extensionCount = envelope.Extensions?.Count ?? 0;
        NatsHeaders headers = new (extensionCount + 10);

        if (envelope.Extensions is not null)
        {
            foreach (var extension in envelope.Extensions)
            {
                headers[GetCloudEventsHeaderName(extension.Key)] = extension.Value;
            }
        }

        headers[GetCloudEventsHeaderName(CloudEventAttributeNames.Id)] = envelope.Id;
        headers[GetCloudEventsHeaderName(CloudEventAttributeNames.SpecVersion)] = envelope.SpecVersion;
        headers[GetCloudEventsHeaderName(CloudEventAttributeNames.Source)] = envelope.Source;
        headers[GetCloudEventsHeaderName(CloudEventAttributeNames.Type)] = envelope.Type;
        headers[GetCloudEventsHeaderName(CloudEventAttributeNames.Time)] =
            envelope.Time.ToString("O", CultureInfo.InvariantCulture);
        headers["message-id"] = envelope.Id;

        if (envelope.DataContentType is not null)
        {
            headers["content-type"] = envelope.DataContentType;
        }

        if (envelope.Subject is not null)
        {
            headers[GetCloudEventsHeaderName(CloudEventAttributeNames.Subject)] = envelope.Subject;
        }

        if (envelope.DataSchema is not null)
        {
            headers[GetCloudEventsHeaderName(CloudEventAttributeNames.DataSchema)] = envelope.DataSchema;
        }

        InjectTraceContext(headers);
        return headers;
    }

    private static void InjectTraceContext(NatsHeaders headers)
    {
        // The W3C propagator only ever writes into the carrier, so a plain dictionary is a sufficient sink;
        // its entries are then copied onto the NATS headers. This avoids adapting NatsHeaders to the full
        // IDictionary surface the propagator's carrier type nominally requires but never exercises.
        Dictionary<string, string?> traceHeaders = new (StringComparer.Ordinal);
        TraceContextHeaders.Inject(traceHeaders);
        foreach (var traceHeader in traceHeaders)
        {
            headers[traceHeader.Key] = traceHeader.Value;
        }
    }

    private static string GetCloudEventsHeaderName(string attributeName)
    {
        return $"{CloudEventsWireHeaderPrefix}{attributeName}";
    }

    private static string MapHeaderName(string headerName)
    {
        return headerName.StartsWith(
            CloudEventsInboundMessageInspector.CloudEventsHeaderPrefix,
            StringComparison.Ordinal
        ) ?
            $"{CloudEventsWireHeaderPrefix}{headerName.Substring(CloudEventsInboundMessageInspector.CloudEventsHeaderPrefix.Length)}" :
            headerName;
    }
}
