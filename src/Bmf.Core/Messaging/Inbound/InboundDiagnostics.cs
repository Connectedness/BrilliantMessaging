using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Bmf.Core.Messaging.Inbound;

/// <summary>
/// The shared <see cref="System.Diagnostics" /> primitives for the inbound path: the activity source, the meter,
/// and the process instruments. Subscribe to <see cref="ActivitySourceName" /> and <see cref="MeterName" /> to
/// observe the framework's inbound consumer-hop telemetry, and use
/// <see cref="Bmf.Core.Messaging.Outbound.OutboundDiagnostics" /> for the corresponding producer-hop telemetry.
/// </summary>
/// <remarks>
/// The consumer span and the <see cref="ConsumedMessages" />/<see cref="OperationDuration" /> instruments are
/// annotated with the OpenTelemetry <c>messaging.*</c> semantic conventions defined in
/// <see cref="Bmf.Core.Messaging.MessagingSemanticConventions" /> (pinned to
/// <see cref="Bmf.Core.Messaging.MessagingSemanticConventions.SemanticConventionsVersion" />):
/// <c>messaging.system</c>, <c>messaging.operation.type</c>=<c>process</c> (and <c>messaging.operation.name</c>),
/// <c>messaging.destination.name</c> (the consumed source), <c>messaging.rabbitmq.destination.routing_key</c> when
/// present, <c>messaging.message.id</c>, <c>messaging.message.body.size</c>, and <c>error.type</c> on failure.
/// </remarks>
public static class InboundDiagnostics
{
    /// <summary>
    /// The name of the activity source for inbound telemetry.
    /// </summary>
    public const string ActivitySourceName = "Bmf.Inbound";

    /// <summary>
    /// The name of the meter for inbound telemetry. It currently equals <see cref="ActivitySourceName" />, but is a
    /// separate constant so the meter name can diverge later without touching subscribers (such as the
    /// <c>Bmf.OpenTelemetry</c> registration package).
    /// </summary>
    public const string MeterName = "Bmf.Inbound";

    /// <summary>
    /// The activity source that emits inbound consumer activities.
    /// </summary>
    public static readonly ActivitySource ActivitySource = new (ActivitySourceName);

    /// <summary>
    /// The meter that owns the inbound instruments.
    /// </summary>
    public static readonly Meter Meter = new (MeterName);

    /// <summary>
    /// Counts messages delivered to the application (<c>messaging.client.consumed.messages</c>). A failed delivery is
    /// the same measurement carrying an <c>error.type</c> dimension, so the failure rate is the
    /// <c>error.type</c>-present slice rather than a separate counter. Each measurement represents one delivery that
    /// reached the framework pipeline, or one pre-pipeline reject counted by the transport runtime.
    /// </summary>
    public static readonly Counter<long> ConsumedMessages =
        Meter.CreateCounter<long>("messaging.client.consumed.messages", unit: "{message}");

    /// <summary>
    /// Records process operation durations in seconds (<c>messaging.client.operation.duration</c>), tagged with the
    /// same <c>messaging.*</c> and (on failure) <c>error.type</c> dimensions as <see cref="ConsumedMessages" />.
    /// </summary>
    public static readonly Histogram<double> OperationDuration =
        Meter.CreateHistogram<double>("messaging.client.operation.duration", unit: "s");
}
