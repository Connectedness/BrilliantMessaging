using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Threading;
using BrilliantMessaging.Core.Messaging.Inbound;

namespace BrilliantMessaging.Transport.Nats.Tests.TestSupport;

public sealed class InboundDiagnosticsRecorder : IDisposable
{
    public const string ConsumedMessagesInstrumentName = "messaging.client.consumed.messages";

    private readonly ActivityListener _activityListener;
    private readonly Lock _gate = new ();
    private readonly MeterListener _meterListener;

    public InboundDiagnosticsRecorder()
    {
        // The listener stays registered even though nothing records the activities: without a listener
        // ActivitySource.StartActivity returns null, which changes the code path the production
        // diagnostics take and therefore the tags the consumed-messages assertions check.
        _activityListener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == InboundDiagnostics.ActivitySourceName,
            Sample = static (ref _) => ActivitySamplingResult.AllData
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
                    lock (_gate)
                    {
                        ConsumedMessages.Add(tags.ToArray());
                    }
                }
            }
        );
        _meterListener.Start();
    }

    public List<KeyValuePair<string, object?>[]> ConsumedMessages { get; } = [];

    public void Dispose()
    {
        _meterListener.Dispose();
        _activityListener.Dispose();
    }

    public KeyValuePair<string, object?>[][] SnapshotConsumedMessages()
    {
        lock (_gate)
        {
            return ConsumedMessages.ToArray();
        }
    }
}
