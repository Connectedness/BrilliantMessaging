using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Bmf.Core.Messaging.Inbound;

/// <summary>
/// Inbound middleware that opens the consumer-hop span and records inbound process metrics. It extracts W3C
/// trace context from the transport headers, parents the <see cref="ActivityKind.Consumer" /> activity to the
/// producer span carried over the broker, and makes that activity current while acknowledgement, deserialization,
/// user middleware, and the handler run. Register it outermost so handler-initiated publishes automatically
/// become children of the consumer span and so the metrics cover the whole framework pipeline.
/// </summary>
public sealed class InboundDiagnosticsMiddleware : IMessageMiddleware
{
    private const string ProcessActivityName = "bmf.inbound.process";

    /// <inheritdoc />
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="context" /> or <paramref name="next" /> is <see langword="null" />.</exception>
    public async Task InvokeAsync(IncomingMessageContext context, MessageDelegate next)
    {
        if (context is null)
        {
            throw new ArgumentNullException(nameof(context));
        }

        if (next is null)
        {
            throw new ArgumentNullException(nameof(next));
        }

        var baseTags = new TagList
        {
            { InboundDiagnostics.MessageTypeTagName, context.Endpoint.Discriminator },
            { InboundDiagnostics.EndpointNameTagName, context.Endpoint.Name },
            { InboundDiagnostics.SourceTagName, context.Transport.Source },
            { InboundDiagnostics.TransportNameTagName, context.Transport.TransportName }
        };

        var traceContext = TraceContextHeaders.Extract(context.Transport);
        using var activity = InboundDiagnostics.ActivitySource.StartActivity(
            ProcessActivityName,
            ActivityKind.Consumer,
            traceContext.TraceParent
        );

        if (activity is not null)
        {
            activity.TraceStateString = traceContext.TraceState;
            foreach (var baggage in traceContext.Baggage)
            {
                activity.AddBaggage(baggage.Key, baggage.Value);
            }

            foreach (var tag in baseTags)
            {
                activity.SetTag(tag.Key, tag.Value);
            }
        }

        var outcome = "success";
        var startedTimestamp = Stopwatch.GetTimestamp();
        InboundDiagnostics.ProcessAttempts.Add(1, baseTags);
        try
        {
            await next(context).ConfigureAwait(false);
            activity?.SetStatus(ActivityStatusCode.Ok);
        }
        catch (OperationCanceledException) when (context.CancellationToken.IsCancellationRequested)
        {
            outcome = "cancelled";
            throw;
        }
        catch (Exception exception)
        {
            outcome = "failure";
            var failureTags = BuildOutcomeTags(baseTags, outcome);
            InboundDiagnostics.ProcessFailures.Add(1, failureTags);
            activity?.SetStatus(ActivityStatusCode.Error);
            RecordException(activity, exception);
            throw;
        }
        finally
        {
            var outcomeTags = BuildOutcomeTags(baseTags, outcome);
            InboundDiagnostics.ProcessDuration.Record(GetDurationMilliseconds(startedTimestamp), outcomeTags);
            activity?.SetTag(InboundDiagnostics.OutcomeTagName, outcome);
        }
    }

    private static TagList BuildOutcomeTags(TagList baseTags, string outcome)
    {
        var tags = baseTags;
        tags.Add(InboundDiagnostics.OutcomeTagName, outcome);
        return tags;
    }

    private static double GetDurationMilliseconds(long startedTimestamp)
    {
        var elapsedTicks = Stopwatch.GetTimestamp() - startedTimestamp;
        return elapsedTicks * 1000d / Stopwatch.Frequency;
    }

    private static void RecordException(Activity? activity, Exception exception)
    {
        if (activity is null)
        {
            return;
        }

        var tags = new ActivityTagsCollection
        {
            { "exception.type", exception.GetType().FullName },
            { "exception.message", exception.Message }
        };

        if (exception.StackTrace is not null)
        {
            tags.Add("exception.stacktrace", exception.StackTrace);
        }

        activity.AddEvent(new ActivityEvent("exception", tags: tags));
    }
}
