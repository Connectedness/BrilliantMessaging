using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Usf.Core.Messaging;

namespace Usf.Core.Tests.Messaging.TestSupport;

/// <summary>
/// Captures the process-global outbound publish diagnostics (the <see cref="OutboundDiagnostics" />
/// activity source and meter) for the duration of a single test. Tests that use this recorder must run in
/// the <c>Diagnostics</c> collection so that captured activities and measurements are never contaminated by
/// publishes happening on other threads.
/// </summary>
public sealed class OutboundDiagnosticsRecorder : IDisposable
{
    public const string AttemptsInstrumentName = "usf.outbound.publish.attempts";

    public const string FailuresInstrumentName = "usf.outbound.publish.failures";

    public const string DurationInstrumentName = "usf.outbound.publish.duration";

    private readonly ActivityListener _activityListener;
    private readonly MeterListener _meterListener;

    public OutboundDiagnosticsRecorder()
    {
        _activityListener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == OutboundDiagnostics.ActivitySourceName,
            Sample = static (ref _) => ActivitySamplingResult.AllData,
            ActivityStarted = activity => StartedActivities.Add(activity)
        };
        ActivitySource.AddActivityListener(_activityListener);

        _meterListener = new MeterListener
        {
            InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == OutboundDiagnostics.ActivitySourceName)
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            }
        };
        _meterListener.SetMeasurementEventCallback<long>(
            (instrument, _, tags, _) =>
            {
                switch (instrument.Name)
                {
                    case AttemptsInstrumentName:
                        Attempts.Add(tags.ToArray());
                        break;
                    case FailuresInstrumentName:
                        Failures.Add(tags.ToArray());
                        break;
                }
            }
        );
        _meterListener.SetMeasurementEventCallback<double>(
            (instrument, _, tags, _) =>
            {
                if (instrument.Name == DurationInstrumentName)
                {
                    Durations.Add(tags.ToArray());
                }
            }
        );
        _meterListener.Start();
    }

    public List<Activity> StartedActivities { get; } = [];

    public List<KeyValuePair<string, object?>[]> Attempts { get; } = [];

    public List<KeyValuePair<string, object?>[]> Failures { get; } = [];

    public List<KeyValuePair<string, object?>[]> Durations { get; } = [];

    public void Dispose()
    {
        _meterListener.Dispose();
        _activityListener.Dispose();
    }
}
