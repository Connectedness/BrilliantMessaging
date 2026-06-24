using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using BrilliantMessaging.Core.Messaging.Inbound;

namespace BrilliantMessaging.Transport.RabbitMq.Tests.TestSupport;

public sealed class InboundDiagnosticsRecorder : IDisposable
{
    public const string ConsumedMessagesInstrumentName = "messaging.client.consumed.messages";

    public const string OperationDurationInstrumentName = "messaging.client.operation.duration";

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
                if (instrument.Meter.Name == InboundDiagnostics.MeterName)
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            }
        };
        _meterListener.SetMeasurementEventCallback<long>(
            (instrument, _, tags, _) =>
            {
                if (instrument.Name == ConsumedMessagesInstrumentName)
                {
                    ConsumedMessages.Add(tags.ToArray());
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

    public List<Activity> StoppedActivities { get; } = [];

    public List<KeyValuePair<string, object?>[]> ConsumedMessages { get; } = [];

    public List<KeyValuePair<string, object?>[]> Durations { get; } = [];

    public void Dispose()
    {
        _meterListener.Dispose();
        _activityListener.Dispose();
    }
}
