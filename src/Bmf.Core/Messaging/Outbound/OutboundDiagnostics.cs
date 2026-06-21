using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Bmf.Core.Messaging.Outbound;

/// <summary>
/// The shared <see cref="System.Diagnostics" /> primitives for the outbound path: the activity source, the meter,
/// the publish and topology-provisioning instruments. Subscribe to <see cref="ActivitySourceName" /> and
/// <see cref="MeterName" /> to observe the framework's outbound telemetry.
/// </summary>
/// <remarks>
/// The publish span is annotated with the OpenTelemetry <c>messaging.*</c> semantic conventions defined in
/// <see cref="MessagingSemanticConventions" /> (pinned to
/// <see cref="MessagingSemanticConventions.SemanticConventionsVersion" />): <c>messaging.system</c>,
/// <c>messaging.operation.type</c>=<c>send</c> (and <c>messaging.operation.name</c>),
/// <c>messaging.destination.name</c> (the exchange), <c>messaging.rabbitmq.destination.routing_key</c> when present,
/// <c>messaging.message.id</c>, <c>messaging.message.body.size</c>, and <c>error.type</c> on failure. The
/// <see cref="SentMessages" />/<see cref="OperationDuration" /> instruments use only the low-cardinality metric
/// dimensions: <c>messaging.system</c>, <c>messaging.operation.name</c>, <c>messaging.destination.name</c>, and
/// <c>error.type</c> on failure. The topology-provisioning instruments are intentionally outside the messaging conventions and keep their
/// <c>bmf.outbound.topology.provisioning.*</c> names. Use <see cref="Bmf.Core.Messaging.Inbound.InboundDiagnostics" />
/// for the corresponding consumer-hop telemetry.
/// </remarks>
public static class OutboundDiagnostics
{
    /// <summary>
    /// The name of the activity source for outbound telemetry.
    /// </summary>
    public const string ActivitySourceName = "Bmf.Outbound";

    /// <summary>
    /// The name of the meter for outbound telemetry. It currently equals <see cref="ActivitySourceName" />, but is a
    /// separate constant so the meter name can diverge later without touching subscribers (such as the
    /// <c>Bmf.OpenTelemetry</c> registration package).
    /// </summary>
    public const string MeterName = "Bmf.Outbound";

    /// <summary>
    /// The activity source that emits outbound publish activities.
    /// </summary>
    public static readonly ActivitySource ActivitySource = new (ActivitySourceName);

    /// <summary>
    /// The meter that owns the outbound instruments.
    /// </summary>
    public static readonly Meter Meter = new (MeterName);

    /// <summary>
    /// Counts messages the producer attempted to send (<c>messaging.client.sent.messages</c>). A failed publish is
    /// the same measurement carrying an <c>error.type</c> dimension, so the failure rate is the
    /// <c>error.type</c>-present slice rather than a separate counter.
    /// </summary>
    public static readonly Counter<long> SentMessages =
        Meter.CreateCounter<long>("messaging.client.sent.messages", unit: "{message}");

    /// <summary>
    /// Records publish operation durations in seconds (<c>messaging.client.operation.duration</c>), tagged with the
    /// same <c>messaging.*</c> and (on failure) <c>error.type</c> dimensions as <see cref="SentMessages" />.
    /// </summary>
    public static readonly Histogram<double> OperationDuration =
        Meter.CreateHistogram<double>("messaging.client.operation.duration", unit: "s");

    /// <summary>
    /// Counts topology-provisioning attempts. Provisioning is not a messaging operation, so this instrument keeps the
    /// <c>bmf.outbound.*</c> scheme and is outside the <c>messaging.*</c> conventions.
    /// </summary>
    public static readonly Counter<long> TopologyProvisioningAttempts =
        Meter.CreateCounter<long>("bmf.outbound.topology.provisioning.attempts");

    /// <summary>
    /// Counts topology-provisioning failures. Provisioning is not a messaging operation, so this instrument keeps the
    /// <c>bmf.outbound.*</c> scheme and is outside the <c>messaging.*</c> conventions.
    /// </summary>
    public static readonly Counter<long> TopologyProvisioningFailures =
        Meter.CreateCounter<long>("bmf.outbound.topology.provisioning.failures");

    /// <summary>
    /// Records topology-provisioning durations in milliseconds. Provisioning is not a messaging operation, so this
    /// instrument keeps the <c>bmf.outbound.*</c> scheme and is outside the <c>messaging.*</c> conventions.
    /// </summary>
    public static readonly Histogram<double> TopologyProvisioningDuration = Meter.CreateHistogram<double>(
        "bmf.outbound.topology.provisioning.duration",
        unit: "ms"
    );
}
