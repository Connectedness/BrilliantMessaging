using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using BrilliantMessaging.Core.Messaging.Outbound;

namespace BrilliantMessaging.Core.Tests.Messaging.TestSupport;

/// <summary>
/// Captures the process-global outbound publish diagnostics (the <see cref="OutboundDiagnostics" />
/// activity source and meter) for the duration of a single test. Tests that use this recorder must run in
/// the <c>Diagnostics</c> collection so that captured activities and measurements are never contaminated by
/// publishes happening on other threads.
/// </summary>
public sealed class OutboundDiagnosticsRecorder : IDisposable
{
    public const string SentMessagesInstrumentName = "messaging.client.sent.messages";

    public const string OperationDurationInstrumentName = "messaging.client.operation.duration";

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
                if (instrument.Meter.Name == OutboundDiagnostics.MeterName)
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            }
        };
        _meterListener.SetMeasurementEventCallback<long>(
            (instrument, _, tags, _) =>
            {
                if (instrument.Name == SentMessagesInstrumentName)
                {
                    SentMessages.Add(tags.ToArray());
                }
            }
        );
        _meterListener.SetMeasurementEventCallback<double>(
            (instrument, _, tags, _) =>
            {
                if (instrument.Name == OperationDurationInstrumentName)
                {
                    Durations.Add(tags.ToArray());
                }
            }
        );
        _meterListener.Start();
    }

    public List<Activity> StartedActivities { get; } = [];

    public List<KeyValuePair<string, object?>[]> SentMessages { get; } = [];

    public List<KeyValuePair<string, object?>[]> Durations { get; } = [];

    public void Dispose()
    {
        _meterListener.Dispose();
        _activityListener.Dispose();
    }
}
