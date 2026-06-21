using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Bmf.Core.Messaging.Inbound;

/// <summary>
/// The shared <see cref="System.Diagnostics" /> primitives for the inbound path: the activity source, the meter,
/// the process instruments, and the tag names used to annotate them. Subscribe to
/// <see cref="ActivitySourceName" /> to observe the framework's inbound consumer-hop telemetry, and use
/// <see cref="Bmf.Core.Messaging.Outbound.OutboundDiagnostics" /> for the corresponding producer-hop telemetry.
/// </summary>
public static class InboundDiagnostics
{
    /// <summary>
    /// The name of the activity source and meter for inbound telemetry.
    /// </summary>
    public const string ActivitySourceName = "Bmf.Inbound";

    /// <summary>
    /// The tag name carrying the inbound message type, represented by its CloudEvents discriminator.
    /// </summary>
    public const string MessageTypeTagName = "bmf.inbound.message.type";

    /// <summary>
    /// The tag name carrying the logical inbound endpoint name that processed the delivery.
    /// </summary>
    public const string EndpointNameTagName = "bmf.inbound.endpoint.name";

    /// <summary>
    /// The tag name carrying the transport source that delivered the message, such as a queue name.
    /// </summary>
    public const string SourceTagName = "bmf.inbound.source";

    /// <summary>
    /// The tag name carrying the transport name.
    /// </summary>
    public const string TransportNameTagName = "bmf.inbound.transport.name";

    /// <summary>
    /// The tag name carrying the processing outcome (success, failure, or cancelled).
    /// </summary>
    public const string OutcomeTagName = "bmf.inbound.outcome";

    /// <summary>
    /// The activity source that emits inbound consumer activities.
    /// </summary>
    public static readonly ActivitySource ActivitySource = new (ActivitySourceName);

    /// <summary>
    /// The meter that owns the inbound instruments.
    /// </summary>
    public static readonly Meter Meter = new (ActivitySourceName);

    /// <summary>
    /// Counts inbound process attempts. Each measurement represents one delivery that reached the framework
    /// pipeline, or one pre-pipeline reject counted by the transport runtime.
    /// </summary>
    public static readonly Counter<long> ProcessAttempts =
        Meter.CreateCounter<long>("bmf.inbound.process.attempts");

    /// <summary>
    /// Counts inbound process failures. Each measurement represents one failed delivery and is also represented
    /// by a corresponding <see cref="ProcessAttempts" /> measurement.
    /// </summary>
    public static readonly Counter<long> ProcessFailures =
        Meter.CreateCounter<long>("bmf.inbound.process.failures");

    /// <summary>
    /// Records inbound process durations in milliseconds.
    /// </summary>
    public static readonly Histogram<double> ProcessDuration =
        Meter.CreateHistogram<double>("bmf.inbound.process.duration", unit: "ms");
}
