using System.Threading;
using System.Threading.Tasks;

namespace Usf.Core.Messaging;

/// <summary>
/// Capability that an <see cref="OutboundTarget{T}" /> exposes when caller-supplied routing keys are
/// meaningful for it. The routing-key publish surface lives here, intentionally hidden from the
/// routing-key-free <see cref="OutboundTarget{T}" /> base so that misuse fails to compile rather than
/// being silently ignored. All overloads require a non-blank routing key; callers that want the
/// target's default routing behavior publish through <see cref="OutboundTarget{T}" /> instead.
/// </summary>
public interface IOutboundRoutableTarget<in T>
{
    Task PublishAsync(
        T message,
        string routingKey,
        CancellationToken cancellationToken = default
    );

    Task PublishAsync(
        T message,
        in CloudEventMetadata metadata,
        string routingKey,
        CancellationToken cancellationToken = default
    );

    Task PublishAsync(
        T message,
        in CloudEventMetadata metadata,
        string type,
        string? dataSchema,
        string routingKey,
        CancellationToken cancellationToken = default
    );
}
