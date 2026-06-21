using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Bmf.Core.Messaging.Inbound;

namespace Bmf.Transport.RabbitMq.Tests.TestSupport;

public sealed class InboundDiagnosticsRecorder : IDisposable
{
    public const string AttemptsInstrumentName = "bmf.inbound.process.attempts";

    public const string FailuresInstrumentName = "bmf.inbound.process.failures";

    public const string DurationInstrumentName = "bmf.inbound.process.duration";

    private readonly ActivityListener _activityListener;
    private readonly MeterListener _meterListener;

    public InboundDiagnosticsRecorder()
    {
        _activityListener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == InboundDiagnostics.ActivitySourceName,
            Sample = static (ref _) => ActivitySamplingResult.AllData,
            ActivityStopped = activity => StoppedActivities.Add(activity)
        };
        ActivitySource.AddActivityListener(_activityListener);

        _meterListener = new MeterListener
        {
            InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == InboundDiagnostics.ActivitySourceName)
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

    public List<Activity> StoppedActivities { get; } = [];

    public List<KeyValuePair<string, object?>[]> Attempts { get; } = [];

    public List<KeyValuePair<string, object?>[]> Failures { get; } = [];

    public List<KeyValuePair<string, object?>[]> Durations { get; } = [];

    public void Dispose()
    {
        _meterListener.Dispose();
        _activityListener.Dispose();
    }
}
