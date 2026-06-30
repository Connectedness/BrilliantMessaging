using System;
using System.Collections;
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
        await jetStream
           .PublishAsync(
                _subject,
                message.Body,
                serializer: null,
                options,
                headers,
                cancellationToken
            )
           .ConfigureAwait(false);
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
        await jetStream
           .PublishAsync(
                _subject,
                envelope.Data.ToArray(),
                serializer: null,
                options,
                headers,
                cancellationToken
            )
           .ConfigureAwait(false);
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

        TraceContextHeaders.Inject(new NatsHeaderDictionary(headers));
        return headers;
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

    private sealed class NatsHeaderDictionary : IDictionary<string, object?>
    {
        private readonly NatsHeaders _headers;

        public NatsHeaderDictionary(NatsHeaders headers)
        {
            _headers = headers;
        }

        public object? this[string key]
        {
            get => _headers.TryGetValue(key, out var value) ? value.ToString() : null;
            set => _headers[key] = value?.ToString();
        }

        public ICollection<string> Keys => _headers.Keys;
        public ICollection<object?> Values => throw new NotSupportedException();
        public int Count => _headers.Count;
        public bool IsReadOnly => false;
        public void Add(string key, object? value) => _headers.Add(key, value?.ToString());
        public void Add(KeyValuePair<string, object?> item) => Add(item.Key, item.Value);
        public void Clear() => _headers.Clear();
        public bool Contains(KeyValuePair<string, object?> item) => _headers.ContainsKey(item.Key);
        public bool ContainsKey(string key) => _headers.ContainsKey(key);
        public void CopyTo(KeyValuePair<string, object?>[] array, int arrayIndex) => throw new NotSupportedException();

        public IEnumerator<KeyValuePair<string, object?>> GetEnumerator()
        {
            foreach (var header in _headers)
            {
                yield return new KeyValuePair<string, object?>(header.Key, header.Value.ToString());
            }
        }

        public bool Remove(string key) => _headers.Remove(key);
        public bool Remove(KeyValuePair<string, object?> item) => Remove(item.Key);

        public bool TryGetValue(string key, out object? value)
        {
            if (_headers.TryGetValue(key, out var headerValue))
            {
                value = headerValue.ToString();
                return true;
            }

            value = null;
            return false;
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
