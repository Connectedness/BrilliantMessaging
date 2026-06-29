using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Abstractions;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Outbound;
using BrilliantMessaging.Transport.InMemory.Inbound;

namespace BrilliantMessaging.Transport.InMemory.Outbound;

/// <summary>
/// The in-memory outbound target for a declared topic. It applies the CloudEvents v1.0 binary content-mode binding
/// by carrying the CloudEvents attributes as <c>cloudEvents:</c>-prefixed headers, then routes the serialized
/// delivery onto the broker's background queues for the topic. Publishing never invokes consumers inline.
/// </summary>
/// <typeparam name="TMessage">The message type the target publishes.</typeparam>
public sealed class InMemoryOutboundTarget<TMessage> : OutboundTarget<TMessage>
{
    /// <summary>
    /// Prefixes CloudEvents attributes carried as headers by the binary content-mode binding.
    /// </summary>
    public const string CloudEventsHeaderPrefix = "cloudEvents:";

    private readonly InMemoryBroker _broker;
    private readonly string _topic;

    /// <summary>
    /// Initializes a new instance of the <see cref="InMemoryOutboundTarget{TMessage}" /> class.
    /// </summary>
    /// <param name="name">The logical name of the target.</param>
    /// <param name="serializer">The serializer used to turn messages into CloudEvents envelopes.</param>
    /// <param name="messageContractRegistry">The registry used to resolve discriminators and data schemas.</param>
    /// <param name="topologyName">The name of the topology the target belongs to.</param>
    /// <param name="topic">The declared topic published messages are routed to.</param>
    /// <param name="broker">The broker that records and dispatches routed messages.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="broker" /> is <see langword="null" />.</exception>
    /// <exception cref="ArgumentException">Thrown when <paramref name="topic" /> is null or whitespace.</exception>
    public InMemoryOutboundTarget(
        string name,
        IMessageSerializer serializer,
        IMessageContractRegistry messageContractRegistry,
        string topologyName,
        string topic,
        InMemoryBroker broker
    )
        : base(name, InMemoryInboundEndpoint.TransportNameValue, serializer, messageContractRegistry, topologyName)
    {
        if (string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(topic));
        }

        _topic = topic;
        _broker = broker ?? throw new ArgumentNullException(nameof(broker));
        DestinationName = topic;
    }

    /// <inheritdoc />
    protected override string? DestinationName { get; }

    /// <inheritdoc />
    protected override Task PublishSerializedCoreAsync(SerializedMessage message, CancellationToken cancellationToken)
    {
        Dictionary<string, object?> headers = new (message.Headers.Count, StringComparer.Ordinal);
        foreach (var header in message.Headers)
        {
            headers[header.Key] = header.Value;
        }

        return _broker.RouteAsync(_topic, message.Body, headers, message.ContentType, message.MessageId);
    }

    /// <inheritdoc />
    protected override Task PublishTypedCloudEventAsync(
        TMessage message,
        CloudEventEnvelope envelope,
        string? routingKey,
        CancellationToken cancellationToken
    )
    {
        var headers = CreateHeaders(envelope);
        var body = envelope.Data.ToArray();
        return _broker.RouteAsync(_topic, body, headers, envelope.DataContentType, envelope.Id);
    }

    private static Dictionary<string, object?> CreateHeaders(CloudEventEnvelope envelope)
    {
        var extensionCount = envelope.Extensions?.Count ?? 0;
        Dictionary<string, object?> headers = new (extensionCount + 10, StringComparer.Ordinal);

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
        headers[GetCloudEventsHeaderName(CloudEventAttributeNames.Time)] = envelope.Time.ToString("O");

        if (envelope.Subject is not null)
        {
            headers[GetCloudEventsHeaderName(CloudEventAttributeNames.Subject)] = envelope.Subject;
        }

        if (envelope.DataSchema is not null)
        {
            headers[GetCloudEventsHeaderName(CloudEventAttributeNames.DataSchema)] = envelope.DataSchema;
        }

        TraceContextHeaders.Inject(headers);

        return headers;
    }

    private static string GetCloudEventsHeaderName(string attributeName)
    {
        return $"{CloudEventsHeaderPrefix}{attributeName}";
    }
}
