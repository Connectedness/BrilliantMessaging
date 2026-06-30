using System;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Outbound;

namespace BrilliantMessaging.Transport.Nats.Outbound;

/// <summary>
/// Fluent builder for a NATS outbound target. Publishing is JetStream-backed and awaits the server publish
/// acknowledgement.
/// </summary>
public sealed class NatsOutboundTargetBuilder<TMessage> : IBuildable<NatsOutboundTargetDefinition>
{
    private readonly string? _targetName;
    private bool _messageIdDeduplication;
    private Type? _serializerType;
    private string? _subject;

    /// <summary>
    /// Initializes a new instance of the <see cref="NatsOutboundTargetBuilder{TMessage}" /> class.
    /// </summary>
    public NatsOutboundTargetBuilder(string? targetName = null)
    {
        _targetName = targetName;
    }

    /// <inheritdoc />
    NatsOutboundTargetDefinition IBuildable<NatsOutboundTargetDefinition>.Build()
    {
        if (_subject is null)
        {
            throw new InvalidOperationException("A NATS outbound target must select a subject with ToSubject(...).");
        }

        return new NatsOutboundTargetDefinition(
            typeof(TMessage),
            _subject,
            _targetName,
            _serializerType,
            _messageIdDeduplication
        );
    }

    /// <summary>
    /// Routes messages to a literal NATS subject. Wildcards are not allowed for publish subjects.
    /// </summary>
    public NatsOutboundTargetBuilder<TMessage> ToSubject(string subject)
    {
        _subject = RequireText(subject, nameof(subject));
        return this;
    }

    /// <summary>
    /// Overrides the serializer used by this target.
    /// </summary>
    public NatsOutboundTargetBuilder<TMessage> WithSerializer<TSerializer>()
        where TSerializer : class, IMessageSerializer
    {
        _serializerType = typeof(TSerializer);
        return this;
    }

    /// <summary>
    /// Enables JetStream message-id deduplication by setting <c>Nats-Msg-Id</c> to the CloudEvents id. The stream
    /// must also configure an appropriate duplicate window for effective once-only publish acceptance.
    /// </summary>
    public NatsOutboundTargetBuilder<TMessage> UseMessageIdDeduplication(bool enabled = true)
    {
        _messageIdDeduplication = enabled;
        return this;
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
