using System.Threading;
using System.Threading.Tasks;

namespace Bmf.Core.Messaging.Outbound;

/// <summary>
/// Capability that an <see cref="OutboundTarget{T}" /> exposes when caller-supplied routing keys are
/// meaningful for it. The routing-key publish surface lives here, intentionally hidden from the
/// routing-key-free <see cref="OutboundTarget{T}" /> base so that misuse fails to compile rather than
/// being silently ignored. All overloads require a non-blank routing key; callers that want the
/// target's default routing behavior publish through <see cref="OutboundTarget{T}" /> instead.
/// </summary>
public interface IOutboundRoutableTarget<in T>
{
    /// <summary>
    /// Publishes a message with an explicit routing key, deriving the CloudEvents metadata from the message.
    /// </summary>
    /// <param name="message">The message to publish.</param>
    /// <param name="routingKey">The transport routing key; must be non-blank.</param>
    /// <param name="cancellationToken">A token to observe while waiting for the publish to complete.</param>
    /// <returns>A task that completes when the transport has accepted the message.</returns>
    Task PublishAsync(
        T message,
        string routingKey,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Publishes a message with an explicit routing key and explicit CloudEvents metadata.
    /// </summary>
    /// <param name="message">The message to publish.</param>
    /// <param name="metadata">The CloudEvents metadata to attach.</param>
    /// <param name="routingKey">The transport routing key; must be non-blank.</param>
    /// <param name="cancellationToken">A token to observe while waiting for the publish to complete.</param>
    /// <returns>A task that completes when the transport has accepted the message.</returns>
    Task PublishAsync(
        T message,
        in CloudEventMetadata metadata,
        string routingKey,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Publishes a message with an explicit routing key, CloudEvents metadata, and explicit <c>type</c>/
    /// <c>dataschema</c> attributes.
    /// </summary>
    /// <param name="message">The message to publish.</param>
    /// <param name="metadata">The CloudEvents metadata to attach.</param>
    /// <param name="type">The CloudEvents <c>type</c> attribute to use.</param>
    /// <param name="dataSchema">The CloudEvents <c>dataschema</c> attribute to use, or <see langword="null" /> for none.</param>
    /// <param name="routingKey">The transport routing key; must be non-blank.</param>
    /// <param name="cancellationToken">A token to observe while waiting for the publish to complete.</param>
    /// <returns>A task that completes when the transport has accepted the message.</returns>
    Task PublishAsync(
        T message,
        in CloudEventMetadata metadata,
        string type,
        string? dataSchema,
        string routingKey,
        CancellationToken cancellationToken = default
    );
}
