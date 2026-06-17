using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Bmf.Core.Messaging.Outbound;

/// <summary>
/// The shared <see cref="System.Diagnostics" /> primitives for the outbound path: the activity source, the meter,
/// the publish and topology-provisioning instruments, and the tag names used to annotate them. Subscribe to
/// <see cref="ActivitySourceName" /> to observe the framework's outbound telemetry.
/// </summary>
public static class OutboundDiagnostics
{
    /// <summary>
    /// The name of the activity source and meter for outbound telemetry.
    /// </summary>
    public const string ActivitySourceName = "Bmf.Outbound";

    /// <summary>
    /// The tag name carrying the message type (discriminator) of a publish.
    /// </summary>
    public const string MessageTypeTagName = "bmf.outbound.message.type";

    /// <summary>
    /// The tag name carrying the outbound target name.
    /// </summary>
    public const string TargetNameTagName = "bmf.outbound.target.name";

    /// <summary>
    /// The tag name carrying the transport name.
    /// </summary>
    public const string TransportNameTagName = "bmf.outbound.transport.name";

    /// <summary>
    /// The tag name carrying the publish outcome (success, failure, or cancelled).
    /// </summary>
    public const string OutcomeTagName = "bmf.outbound.outcome";

    /// <summary>
    /// The tag name carrying the delivery-failure reason when a publish fails delivery.
    /// </summary>
    public const string DeliveryFailureReasonTagName = "bmf.outbound.delivery.failure.reason";

    /// <summary>
    /// The activity source that emits outbound publish activities.
    /// </summary>
    public static readonly ActivitySource ActivitySource = new (ActivitySourceName);

    /// <summary>
    /// The meter that owns the outbound instruments.
    /// </summary>
    public static readonly Meter Meter = new (ActivitySourceName);

    /// <summary>
    /// Counts publish attempts.
    /// </summary>
    public static readonly Counter<long> PublishAttempts = Meter.CreateCounter<long>("bmf.outbound.publish.attempts");

    /// <summary>
    /// Counts publish failures.
    /// </summary>
    public static readonly Counter<long> PublishFailures = Meter.CreateCounter<long>("bmf.outbound.publish.failures");

    /// <summary>
    /// Records publish durations in milliseconds.
    /// </summary>
    public static readonly Histogram<double> PublishDuration =
        Meter.CreateHistogram<double>("bmf.outbound.publish.duration", unit: "ms");

    /// <summary>
    /// Counts topology-provisioning attempts.
    /// </summary>
    public static readonly Counter<long> TopologyProvisioningAttempts =
        Meter.CreateCounter<long>("bmf.outbound.topology.provisioning.attempts");

    /// <summary>
    /// Counts topology-provisioning failures.
    /// </summary>
    public static readonly Counter<long> TopologyProvisioningFailures =
        Meter.CreateCounter<long>("bmf.outbound.topology.provisioning.failures");

    /// <summary>
    /// Records topology-provisioning durations in milliseconds.
    /// </summary>
    public static readonly Histogram<double> TopologyProvisioningDuration = Meter.CreateHistogram<double>(
        "bmf.outbound.topology.provisioning.duration",
        unit: "ms"
    );
}
