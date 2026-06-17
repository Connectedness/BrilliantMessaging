using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using Bmf.Abstractions;
using Bmf.Core.Messaging;
using Bmf.Core.Messaging.Outbound;

namespace Bmf.Transport.RabbitMq.Outbound;

/// <summary>
/// The RabbitMQ base for a typed outbound target. It extends the Core <see cref="OutboundTarget{TMessage}" />
/// with the RabbitMQ dispatch: it acquires a channel from the channel group, applies the CloudEvents v1.0 AMQP
/// binary content-mode binding, publishes to the configured exchange, and — when the channel group uses
/// publisher confirms — translates nacks, unroutable returns, and confirm timeouts into a
/// <see cref="MessageDeliveryException" />.
/// </summary>
/// <remarks>
/// Subclasses supply only the routing strategy by overriding the routing-key and route-header hooks; the base
/// class owns serialization (inherited from the Core target), the AMQP header binding, channel acquisition, and
/// confirm handling. <see cref="GetRawRoutingKey" /> and <see cref="GetRawRouteHeaders" /> serve the raw publish
/// path, while <see cref="ResolveRoutingKey" />/<see cref="GetRoutingKey" /> and <see cref="GetRouteHeaders" />
/// serve the typed path.
/// </remarks>
/// <typeparam name="TMessage">The message type the target publishes.</typeparam>
public abstract class RabbitMqOutboundTarget<TMessage> : OutboundTarget<TMessage>
{
    /// <summary>
    /// Prefixes CloudEvents attributes carried as AMQP application headers by the AMQP protocol binding.
    /// </summary>
    public const string CloudEventsHeaderPrefix = "cloudEvents:";

    private readonly RabbitMqOutboundChannelGroup _channelGroup;
    private readonly string _exchangeName;
    private readonly bool _isMandatory;

    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMqOutboundTarget{TMessage}" /> class.
    /// </summary>
    /// <param name="name">The logical name of the target.</param>
    /// <param name="serializer">The serializer used to turn messages into CloudEvents envelopes.</param>
    /// <param name="messageContractRegistry">The registry used to resolve discriminators and data schemas.</param>
    /// <param name="topologyName">The name of the topology the target belongs to.</param>
    /// <param name="channelGroup">The channel group that supplies publish channels.</param>
    /// <param name="exchangeName">The name of the exchange messages are published to.</param>
    /// <param name="isMandatory">Whether published messages are sent with the AMQP mandatory flag.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="channelGroup" /> or <paramref name="exchangeName" /> is <see langword="null" />.</exception>
    protected RabbitMqOutboundTarget(
        string name,
        IMessageSerializer serializer,
        IMessageContractRegistry messageContractRegistry,
        string topologyName,
        RabbitMqOutboundChannelGroup channelGroup,
        string exchangeName,
        bool isMandatory
    )
        : base(name, "rabbitmq", serializer, messageContractRegistry, topologyName)
    {
        _channelGroup = channelGroup ?? throw new ArgumentNullException(nameof(channelGroup));
        _exchangeName = exchangeName ?? throw new ArgumentNullException(nameof(exchangeName));
        _isMandatory = isMandatory;
    }

    /// <inheritdoc />
    protected sealed override Task PublishSerializedCoreAsync(
        SerializedMessage message,
        CancellationToken cancellationToken
    )
    {
        return DispatchAsync(
            message,
            GetRawRoutingKey(),
            GetRawRouteHeaders(),
            cancellationToken
        );
    }

    /// <inheritdoc />
    protected sealed override Task PublishTypedCloudEventAsync(
        TMessage message,
        CloudEventEnvelope envelope,
        string? routingKey,
        CancellationToken cancellationToken
    )
    {
        return DispatchAsync(
            envelope,
            ResolveRoutingKey(message, routingKey),
            GetRouteHeaders(message),
            cancellationToken
        );
    }

    /// <summary>
    /// Returns the routing key for the raw (already-serialized) publish path. The base returns an empty key;
    /// routing-key targets override it.
    /// </summary>
    /// <returns>The routing key to publish with.</returns>
    protected virtual string GetRawRoutingKey()
    {
        return string.Empty;
    }

    /// <summary>
    /// Returns the route headers for the raw publish path. The base returns no headers; headers-exchange targets
    /// override it.
    /// </summary>
    /// <returns>The headers to attach for routing.</returns>
    protected virtual IReadOnlyDictionary<string, object?> GetRawRouteHeaders()
    {
        return EmptyHeaders.Instance;
    }

    /// <summary>
    /// Returns the routing key derived from a message for the typed publish path. The base returns an empty key;
    /// routing-key targets override it.
    /// </summary>
    /// <param name="message">The message being published.</param>
    /// <returns>The routing key to publish with.</returns>
    protected virtual string GetRoutingKey(TMessage message)
    {
        return string.Empty;
    }

    /// <summary>
    /// Resolves the effective routing key for the typed publish path, preferring an explicit per-publish key over
    /// the message-derived <see cref="GetRoutingKey" />.
    /// </summary>
    /// <param name="message">The message being published.</param>
    /// <param name="routingKey">The explicit per-publish routing key, or <see langword="null" />.</param>
    /// <returns>The routing key to publish with.</returns>
    protected virtual string ResolveRoutingKey(TMessage message, string? routingKey)
    {
        return string.IsNullOrWhiteSpace(routingKey) ? GetRoutingKey(message) : routingKey!;
    }

    /// <summary>
    /// Returns the route headers derived from a message for the typed publish path. The base returns no headers;
    /// headers-exchange targets override it.
    /// </summary>
    /// <param name="message">The message being published.</param>
    /// <returns>The headers to attach for routing.</returns>
    protected virtual IReadOnlyDictionary<string, object?> GetRouteHeaders(TMessage message)
    {
        return EmptyHeaders.Instance;
    }

    private async Task DispatchAsync(
        SerializedMessage serializedMessage,
        string routingKey,
        IReadOnlyDictionary<string, object?> routeHeaders,
        CancellationToken cancellationToken
    )
    {
        await DispatchAsync(
                serializedMessage.Body,
                CreateBasicProperties(serializedMessage, routeHeaders),
                routingKey,
                cancellationToken
            )
           .ConfigureAwait(false);
    }

    private async Task DispatchAsync(
        CloudEventEnvelope envelope,
        string routingKey,
        IReadOnlyDictionary<string, object?> routeHeaders,
        CancellationToken cancellationToken
    )
    {
        await DispatchAsync(
                envelope.Data,
                CreateBasicProperties(envelope, routeHeaders),
                routingKey,
                cancellationToken
            )
           .ConfigureAwait(false);
    }

    private async Task DispatchAsync(
        ReadOnlyMemory<byte> body,
        BasicProperties properties,
        string routingKey,
        CancellationToken cancellationToken
    )
    {
        await using var lease = await _channelGroup.AcquireAsync(cancellationToken).ConfigureAwait(false);

        if (_channelGroup.PublisherConfirmMode == RabbitMqPublisherConfirmMode.FireAndForget)
        {
            await PublishAsync(lease.Channel, properties, body, routingKey, cancellationToken)
               .ConfigureAwait(false);
            return;
        }

        using var timeoutCancellationTokenSource =
            new CancellationTokenSource(_channelGroup.PublisherConfirmTimeout);
        using var linkedCancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            timeoutCancellationTokenSource.Token
        );

        try
        {
            await PublishAsync(
                    lease.Channel,
                    properties,
                    body,
                    routingKey,
                    linkedCancellationTokenSource.Token
                )
               .ConfigureAwait(false);
        }
        catch (PublishException exception)
        {
            var reason = exception.IsReturn ?
                MessageDeliveryFailureReason.Returned :
                MessageDeliveryFailureReason.Nacked;
            throw new MessageDeliveryException(Name, reason, exception);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested &&
                                                 timeoutCancellationTokenSource.IsCancellationRequested)
        {
            throw new MessageDeliveryException(Name, MessageDeliveryFailureReason.Timeout);
        }
    }

    private ValueTask PublishAsync(
        IChannel channel,
        BasicProperties properties,
        ReadOnlyMemory<byte> body,
        string routingKey,
        CancellationToken cancellationToken
    )
    {
        return channel.BasicPublishAsync(
            exchange: _exchangeName,
            routingKey: routingKey,
            mandatory: _isMandatory,
            basicProperties: properties,
            body: body,
            cancellationToken: cancellationToken
        );
    }

    private static BasicProperties CreateBasicProperties(
        SerializedMessage serializedMessage,
        IReadOnlyDictionary<string, object?> routeHeaders
    )
    {
        BasicProperties properties = new ()
        {
            ContentType = serializedMessage.ContentType,
            ContentEncoding = serializedMessage.ContentEncoding,
            MessageId = serializedMessage.MessageId,
            CorrelationId = serializedMessage.CorrelationId
        };

        if (routeHeaders.Count == 0 && serializedMessage.Headers.Count == 0)
        {
            return properties;
        }

        Dictionary<string, object?> headers = new (
            routeHeaders.Count + serializedMessage.Headers.Count,
            StringComparer.Ordinal
        );

        foreach (var header in routeHeaders)
        {
            headers[header.Key] = header.Value;
        }

        foreach (var header in serializedMessage.Headers)
        {
            headers[header.Key] = header.Value;
        }

        properties.Headers = headers;
        return properties;
    }

    /// <summary>
    /// Applies the CloudEvents v1.0 AMQP binary content-mode binding over AMQP 0.9.1.
    /// </summary>
    private static BasicProperties CreateBasicProperties(
        CloudEventEnvelope envelope,
        IReadOnlyDictionary<string, object?> routeHeaders
    )
    {
        BasicProperties properties = new ()
        {
            ContentType = envelope.DataContentType,
            MessageId = envelope.Id
        };

        var extensionCount = envelope.Extensions?.Count ?? 0;
        Dictionary<string, object?> headers = new (
            routeHeaders.Count + extensionCount + 10, // +10 adjusted for tracing
            StringComparer.Ordinal
        );

        foreach (var header in routeHeaders)
        {
            headers[header.Key] = header.Value;
        }

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

        properties.Headers = headers;
        return properties;
    }

    private static string GetCloudEventsHeaderName(string attributeName)
    {
        return $"{CloudEventsHeaderPrefix}{attributeName}";
    }

    private static class EmptyHeaders
    {
        public static readonly IReadOnlyDictionary<string, object?> Instance =
            new Dictionary<string, object?>(0, StringComparer.Ordinal);
    }
}
