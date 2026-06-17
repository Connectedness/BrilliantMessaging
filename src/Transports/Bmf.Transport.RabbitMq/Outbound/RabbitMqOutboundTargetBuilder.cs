using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using Bmf.Core.Messaging.Outbound;

namespace Bmf.Transport.RabbitMq.Outbound;

/// <summary>
/// Fluent builder for a RabbitMQ outbound target for messages of type <typeparamref name="TMessage" />. It
/// captures the exchange and routing strategy (fanout, direct, topic, or headers), the channel group, headers,
/// serializer, and mandatory-routing flag, then compiles them into a <see cref="RabbitMqOutboundTargetDefinition" />.
/// </summary>
/// <typeparam name="TMessage">The message type the target publishes.</typeparam>
public sealed class RabbitMqOutboundTargetBuilder<TMessage>
{
    private readonly Dictionary<string, object?> _headers = new (StringComparer.Ordinal);
    private string? _channelGroupName;
    private string? _exchangeName;
    private string? _routingKey;
    private Func<TMessage, string>? _routingKeyFactory;
    private RabbitMqOutboundRouteScenario _scenario;
    private Type? _serializerType;

    /// <summary>
    /// Gets a value indicating whether the target requests mandatory routing (see <see cref="Mandatory" />).
    /// </summary>
    public bool IsMandatory { get; private set; }

    /// <summary>
    /// Routes published messages to a fanout exchange, which broadcasts to all bound queues regardless of
    /// routing key.
    /// </summary>
    /// <param name="exchangeName">The name of the fanout exchange.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="exchangeName" /> is null or whitespace.</exception>
    public RabbitMqOutboundTargetBuilder<TMessage> ToFanoutExchange(string exchangeName)
    {
        _exchangeName = RequireText(exchangeName, nameof(exchangeName));
        _scenario = RabbitMqOutboundRouteScenario.Fanout;
        _routingKey = null;
        _routingKeyFactory = null;
        _headers.Clear();
        return this;
    }

    /// <summary>
    /// Routes published messages to a direct exchange using a fixed routing key.
    /// </summary>
    /// <param name="exchangeName">The name of the direct exchange.</param>
    /// <param name="routingKey">The fixed routing key; an empty string is permitted.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="exchangeName" /> is null or whitespace.</exception>
    public RabbitMqOutboundTargetBuilder<TMessage> ToDirectExchange(string exchangeName, string routingKey)
    {
        _exchangeName = RequireText(exchangeName, nameof(exchangeName));
        _scenario = RabbitMqOutboundRouteScenario.Direct;
        _routingKey = routingKey ?? string.Empty;
        _routingKeyFactory = null;
        _headers.Clear();
        return this;
    }

    /// <summary>
    /// Routes published messages to a direct exchange using a routing key derived per message.
    /// </summary>
    /// <param name="exchangeName">The name of the direct exchange.</param>
    /// <param name="routingKeyFactory">A function that produces the routing key from the message.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="exchangeName" /> is null or whitespace.</exception>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="routingKeyFactory" /> is <see langword="null" />.</exception>
    public RabbitMqOutboundTargetBuilder<TMessage> ToDirectExchange(
        string exchangeName,
        Func<TMessage, string> routingKeyFactory
    )
    {
        _exchangeName = RequireText(exchangeName, nameof(exchangeName));
        _scenario = RabbitMqOutboundRouteScenario.Direct;
        _routingKey = null;
        _routingKeyFactory = routingKeyFactory ?? throw new ArgumentNullException(nameof(routingKeyFactory));
        _headers.Clear();
        return this;
    }

    /// <summary>
    /// Routes published messages to a topic exchange using a fixed routing key (a dot-delimited topic).
    /// </summary>
    /// <param name="exchangeName">The name of the topic exchange.</param>
    /// <param name="routingKey">The fixed topic routing key; an empty string is permitted.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="exchangeName" /> is null or whitespace.</exception>
    public RabbitMqOutboundTargetBuilder<TMessage> ToTopicExchange(string exchangeName, string routingKey)
    {
        _exchangeName = RequireText(exchangeName, nameof(exchangeName));
        _scenario = RabbitMqOutboundRouteScenario.Topic;
        _routingKey = routingKey ?? string.Empty;
        _routingKeyFactory = null;
        _headers.Clear();
        return this;
    }

    /// <summary>
    /// Routes published messages to a topic exchange using a routing key derived per message.
    /// </summary>
    /// <param name="exchangeName">The name of the topic exchange.</param>
    /// <param name="routingKeyFactory">A function that produces the topic routing key from the message.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="exchangeName" /> is null or whitespace.</exception>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="routingKeyFactory" /> is <see langword="null" />.</exception>
    public RabbitMqOutboundTargetBuilder<TMessage> ToTopicExchange(
        string exchangeName,
        Func<TMessage, string> routingKeyFactory
    )
    {
        _exchangeName = RequireText(exchangeName, nameof(exchangeName));
        _scenario = RabbitMqOutboundRouteScenario.Topic;
        _routingKey = null;
        _routingKeyFactory = routingKeyFactory ?? throw new ArgumentNullException(nameof(routingKeyFactory));
        _headers.Clear();
        return this;
    }

    /// <summary>
    /// Routes published messages to a headers exchange, which matches on the headers configured via
    /// <see cref="WithHeader" /> rather than a routing key.
    /// </summary>
    /// <param name="exchangeName">The name of the headers exchange.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="exchangeName" /> is null or whitespace.</exception>
    public RabbitMqOutboundTargetBuilder<TMessage> ToHeadersExchange(string exchangeName)
    {
        _exchangeName = RequireText(exchangeName, nameof(exchangeName));
        _scenario = RabbitMqOutboundRouteScenario.Headers;
        _routingKey = null;
        _routingKeyFactory = null;
        return this;
    }

    /// <summary>
    /// Publishes through the named channel group instead of the default group, letting the target tune
    /// concurrency and publisher-confirm behaviour independently.
    /// </summary>
    /// <param name="channelGroupName">The name of the channel group to use.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="channelGroupName" /> is null or whitespace.</exception>
    public RabbitMqOutboundTargetBuilder<TMessage> UseChannelGroup(string channelGroupName)
    {
        _channelGroupName = RequireText(channelGroupName, nameof(channelGroupName));
        return this;
    }

    /// <summary>
    /// Adds a header to every message published by the target. For a headers exchange these headers also drive
    /// routing.
    /// </summary>
    /// <param name="name">The header name.</param>
    /// <param name="value">The header value.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="name" /> is null or whitespace.</exception>
    public RabbitMqOutboundTargetBuilder<TMessage> WithHeader(string name, object? value)
    {
        _headers[RequireText(name, nameof(name))] = value;
        return this;
    }

    /// <summary>
    /// Overrides the serializer used by the target with <typeparamref name="TSerializer" /> instead of the
    /// framework default.
    /// </summary>
    /// <typeparam name="TSerializer">The serializer type to use.</typeparam>
    /// <returns>The same builder for chaining.</returns>
    public RabbitMqOutboundTargetBuilder<TMessage> WithSerializer<TSerializer>()
        where TSerializer : class, IMessageSerializer
    {
        _serializerType = typeof(TSerializer);
        return this;
    }

    /// <summary>
    /// Requests a delivery failure when RabbitMQ cannot route a published message to a queue.
    /// </summary>
    /// <remarks>
    /// Mandatory routing requires publisher confirmations on the target's effective channel group so the
    /// returned message can be correlated with its publish. A mandatory target whose effective group uses
    /// <see cref="RabbitMqPublisherConfirmMode.FireAndForget" /> is rejected at compile time
    /// through <see cref="Bmf.Core.Messaging.TopologyValidationException" />; select
    /// <see cref="RabbitMqPublisherConfirmMode.Confirms" /> on the group (see
    /// <c>RabbitMqTopologyBuilder.ChannelGroup</c>) or leave the topology-level default
    /// (<c>WithDefaultPublisherConfirmMode</c>) on confirms. Confirmation tracking serializes outstanding
    /// publishes per channel while awaiting broker outcomes; increase the channel-group size when relaxed
    /// ordering is acceptable and additional throughput is required.
    /// </remarks>
    public RabbitMqOutboundTargetBuilder<TMessage> Mandatory(bool mandatory = true)
    {
        IsMandatory = mandatory;
        return this;
    }

    internal RabbitMqOutboundTargetDefinition Build(string? targetName)
    {
        if (_exchangeName is null)
        {
            throw new InvalidOperationException("A RabbitMQ outbound target must select an exchange.");
        }

        return _scenario switch
        {
            RabbitMqOutboundRouteScenario.Fanout => new RabbitMqFanoutOutboundTargetDefinition(
                typeof(TMessage),
                _exchangeName,
                _channelGroupName,
                targetName,
                _serializerType,
                IsMandatory
            ),
            RabbitMqOutboundRouteScenario.Direct => new RabbitMqDirectOutboundTargetDefinition(
                typeof(TMessage),
                _exchangeName,
                _channelGroupName,
                targetName,
                _serializerType,
                IsMandatory,
                _routingKey,
                _routingKeyFactory
            ),
            RabbitMqOutboundRouteScenario.Topic => new RabbitMqTopicOutboundTargetDefinition(
                typeof(TMessage),
                _exchangeName,
                _channelGroupName,
                targetName,
                _serializerType,
                IsMandatory,
                _routingKey,
                _routingKeyFactory
            ),
            RabbitMqOutboundRouteScenario.Headers => new RabbitMqHeadersOutboundTargetDefinition(
                typeof(TMessage),
                _exchangeName,
                _channelGroupName,
                targetName,
                _serializerType,
                IsMandatory,
                new ReadOnlyDictionary<string, object?>(_headers)
            ),
            _ => throw new ArgumentOutOfRangeException(nameof(_scenario), _scenario, "Unsupported route scenario.")
        };
    }

    private static string RequireText(string value, string parameterName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", parameterName);
        }

        return value;
    }
}
