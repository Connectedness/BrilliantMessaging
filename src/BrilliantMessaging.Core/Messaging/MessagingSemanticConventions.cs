using System;
using BrilliantMessaging.Core.Messaging.Outbound;

namespace BrilliantMessaging.Core.Messaging;

/// <summary>
/// The OpenTelemetry messaging semantic-convention attribute names and well-known values that BrilliantMessaging stamps onto
/// every messaging span and metric, together with the helpers that map a delivery outcome to the bounded
/// <see cref="ErrorType" /> vocabulary. Transports reuse these constants so the producer and consumer paths label
/// their telemetry identically and generic tooling (Jaeger, Tempo, Grafana, Datadog, Azure Monitor) classifies the
/// spans as messaging operations.
/// </summary>
/// <remarks>
/// <para>
/// The attribute names are pinned to OpenTelemetry Semantic Conventions <c>v1.42.0</c>. The messaging group has
/// renamed attributes across releases (for example <c>messaging.operation</c> became
/// <c>messaging.operation.type</c>), so the pinned version is recorded here deliberately: bumping it is a reviewable
/// edit rather than an accidental drift. See the messaging spans
/// (<see href="https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/" />), messaging metrics
/// (<see href="https://opentelemetry.io/docs/specs/semconv/messaging/messaging-metrics/" />), RabbitMQ
/// (<see href="https://opentelemetry.io/docs/specs/semconv/messaging/rabbitmq/" />), and <c>error.type</c>
/// (<see href="https://opentelemetry.io/docs/specs/semconv/attributes-registry/error/" />) pages of the pinned tag
/// <see href="https://github.com/open-telemetry/semantic-conventions/tree/v1.42.0/docs/messaging" />.
/// </para>
/// <para>
/// Only the standardized attribute <em>names</em> are adopted; <c>BrilliantMessaging.Core</c> takes no dependency on any
/// OpenTelemetry package. The optional <c>BrilliantMessaging.OpenTelemetry</c> integration package wires the <c>BrilliantMessaging.Outbound</c>
/// and <c>BrilliantMessaging.Inbound</c> sources and meters into a <c>TracerProvider</c>/<c>MeterProvider</c>.
/// </para>
/// </remarks>
public static class MessagingSemanticConventions
{
    /// <summary>
    /// The pinned OpenTelemetry Semantic Conventions version these constants are taken from.
    /// </summary>
    public const string SemanticConventionsVersion = "1.42.0";

    /// <summary>
    /// <c>messaging.system</c> — the messaging system identifier (for example <c>rabbitmq</c>). Pinned to semantic
    /// conventions <see cref="SemanticConventionsVersion" />.
    /// </summary>
    public const string MessagingSystem = "messaging.system";

    /// <summary>
    /// <c>messaging.operation.type</c> — the messaging operation class, one of the well-known values such as
    /// <see cref="SendOperation" /> or <see cref="ProcessOperation" />. Pinned to semantic conventions
    /// <see cref="SemanticConventionsVersion" />.
    /// </summary>
    public const string MessagingOperationType = "messaging.operation.type";

    /// <summary>
    /// <c>messaging.operation.name</c> — the system-specific name of the messaging operation. Pinned to semantic
    /// conventions <see cref="SemanticConventionsVersion" />.
    /// </summary>
    public const string MessagingOperationName = "messaging.operation.name";

    /// <summary>
    /// <c>messaging.destination.name</c> — the message destination: the exchange on the producer side and the
    /// consumed source (queue) on the consumer side. Pinned to semantic conventions
    /// <see cref="SemanticConventionsVersion" />.
    /// </summary>
    public const string MessagingDestinationName = "messaging.destination.name";

    /// <summary>
    /// <c>messaging.rabbitmq.destination.routing_key</c> — the RabbitMQ routing key, set only when one is present.
    /// Pinned to semantic conventions <see cref="SemanticConventionsVersion" />.
    /// </summary>
    public const string MessagingRabbitMqDestinationRoutingKey = "messaging.rabbitmq.destination.routing_key";

    /// <summary>
    /// <c>messaging.message.id</c> — the message identifier (the CloudEvents <c>id</c>). Pinned to semantic
    /// conventions <see cref="SemanticConventionsVersion" />.
    /// </summary>
    public const string MessagingMessageId = "messaging.message.id";

    /// <summary>
    /// <c>messaging.message.body.size</c> — the size of the message body in bytes. Pinned to semantic conventions
    /// <see cref="SemanticConventionsVersion" />.
    /// </summary>
    public const string MessagingMessageBodySize = "messaging.message.body.size";

    /// <summary>
    /// <c>error.type</c> — the bounded failure-classification dimension, drawn from <see cref="ResolveErrorType" />.
    /// Absent on success and on graceful-shutdown cancellation. Pinned to semantic conventions
    /// <see cref="SemanticConventionsVersion" />.
    /// </summary>
    public const string ErrorType = "error.type";

    /// <summary>
    /// The <c>messaging.operation.type</c>/<c>messaging.operation.name</c> value for a producer publish.
    /// </summary>
    public const string SendOperation = "send";

    /// <summary>
    /// The <c>messaging.operation.type</c>/<c>messaging.operation.name</c> value for a consumer process.
    /// </summary>
    public const string ProcessOperation = "process";

    /// <summary>
    /// The catch-all <c>error.type</c> token for any failure without a known, low-cardinality classification.
    /// </summary>
    public const string ErrorTypeOther = "_OTHER";

    /// <summary>
    /// Maps a failure to its bounded <see cref="ErrorType" /> token: a known
    /// <see cref="MessageDeliveryException" /> maps through <see cref="ToErrorType(MessageDeliveryFailureReason)" />,
    /// and every other exception maps to <see cref="ErrorTypeOther" />. The raw exception type name is never used as
    /// an <see cref="ErrorType" /> value, so the dimension stays safe for metrics; the specific type may be recorded
    /// on the span instead.
    /// </summary>
    /// <param name="exception">The failure to classify.</param>
    /// <returns>The bounded <see cref="ErrorType" /> token.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="exception" /> is <see langword="null" />.</exception>
    public static string ResolveErrorType(Exception exception)
    {
        if (exception is null)
        {
            throw new ArgumentNullException(nameof(exception));
        }

        return exception is MessageDeliveryException deliveryException ?
            ToErrorType(deliveryException.Reason) :
            ErrorTypeOther;
    }

    /// <summary>
    /// Maps a <see cref="MessageDeliveryFailureReason" /> to its stable, low-cardinality
    /// <see cref="ErrorType" /> token (<c>nacked</c>/<c>returned</c>/<c>timeout</c>), falling back to
    /// <see cref="ErrorTypeOther" /> for any unrecognized reason.
    /// </summary>
    /// <param name="reason">The delivery-failure reason.</param>
    /// <returns>The bounded <see cref="ErrorType" /> token.</returns>
    public static string ToErrorType(MessageDeliveryFailureReason reason)
    {
        return reason switch
        {
            MessageDeliveryFailureReason.Nacked => "nacked",
            MessageDeliveryFailureReason.Returned => "returned",
            MessageDeliveryFailureReason.Timeout => "timeout",
            _ => ErrorTypeOther
        };
    }
}
