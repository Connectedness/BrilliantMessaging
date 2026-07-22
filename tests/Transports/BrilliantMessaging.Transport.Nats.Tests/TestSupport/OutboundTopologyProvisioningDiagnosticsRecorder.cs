using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Threading;
using BrilliantMessaging.Core.Messaging.Outbound;

namespace BrilliantMessaging.Transport.Nats.Tests.TestSupport;

public sealed class OutboundTopologyProvisioningDiagnosticsRecorder : IDisposable
{
    public const string AttemptsInstrumentName = "brilliantmessaging.outbound.topology.provisioning.attempts";

    public const string FailuresInstrumentName = "brilliantmessaging.outbound.topology.provisioning.failures";

    public const string DurationInstrumentName = "brilliantmessaging.outbound.topology.provisioning.duration";

    private readonly ActivityListener _activityListener;
    private readonly Lock _gate = new ();
    private readonly MeterListener _meterListener;

    public OutboundTopologyProvisioningDiagnosticsRecorder()
    {
        _activityListener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == OutboundDiagnostics.ActivitySourceName,
            Sample = static (ref _) => ActivitySamplingResult.AllData,
            ActivityStarted = activity =>
            {
                lock (_gate)
                {
                    StartedActivities.Add(activity);
                }
            }
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
                if (instrument.Name == AttemptsInstrumentName)
                {
                    lock (_gate)
                    {
                        Attempts.Add(tags.ToArray());
                    }
                }
                else if (instrument.Name == FailuresInstrumentName)
                {
                    lock (_gate)
                    {
                        Failures.Add(tags.ToArray());
                    }
                }
            }
        );
        _meterListener.SetMeasurementEventCallback<double>(
            (instrument, _, tags, _) =>
            {
                if (instrument.Name == DurationInstrumentName)
                {
                    lock (_gate)
                    {
                        Durations.Add(tags.ToArray());
                    }
                }
            }
        );
        _meterListener.Start();
    }

    private List<Activity> StartedActivities { get; } = [];

    private List<KeyValuePair<string, object?>[]> Attempts { get; } = [];

    private List<KeyValuePair<string, object?>[]> Failures { get; } = [];

    private List<KeyValuePair<string, object?>[]> Durations { get; } = [];

    public void Dispose()
    {
        _meterListener.Dispose();
        _activityListener.Dispose();
    }

    public Activity[] SnapshotStartedActivities()
    {
        lock (_gate)
        {
            return StartedActivities.ToArray();
        }
    }

    public KeyValuePair<string, object?>[][] SnapshotAttempts()
    {
        lock (_gate)
        {
            return Attempts.ToArray();
        }
    }

    public KeyValuePair<string, object?>[][] SnapshotFailures()
    {
        lock (_gate)
        {
            return Failures.ToArray();
        }
    }

    public KeyValuePair<string, object?>[][] SnapshotDurations()
    {
        lock (_gate)
        {
            return Durations.ToArray();
        }
    }
}
