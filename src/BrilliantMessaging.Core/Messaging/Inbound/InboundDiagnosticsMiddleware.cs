using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace BrilliantMessaging.Core.Messaging.Inbound;

/// <summary>
/// Inbound middleware that opens the consumer-hop span and records inbound process metrics, annotated with the
/// OpenTelemetry <c>messaging.*</c> semantic conventions (see
/// <see cref="BrilliantMessaging.Core.Messaging.MessagingSemanticConventions" />). It extracts W3C trace context from the transport
/// headers, parents the <see cref="ActivityKind.Consumer" /> activity to the producer span carried over the broker,
/// and makes that activity current while acknowledgement, deserialization, user middleware, and the handler run.
/// Register it outermost so handler-initiated publishes automatically become children of the consumer span and so the
/// metrics cover the whole framework pipeline.
/// </summary>
/// <remarks>
/// The span is named per the messaging span-name convention (<c>process {destination}</c>) and carries
/// <c>messaging.system</c>, <c>messaging.operation.type</c>=<c>process</c>, <c>messaging.operation.name</c>,
/// <c>messaging.destination.name</c> (the consumed source), <c>messaging.rabbitmq.destination.routing_key</c> when
/// present, <c>messaging.message.id</c>, and <c>messaging.message.body.size</c>. A failure additionally sets a bounded
/// <c>error.type</c> on both the span and the <c>messaging.client.consumed.messages</c> counter, and records the
/// exception on the span. A graceful-shutdown cancellation is not an error: it leaves <c>error.type</c> absent on
/// both the span and the metric.
/// </remarks>
public sealed class InboundDiagnosticsMiddleware : IMessageMiddleware
{
    private const string ProcessActivityName = "brilliantmessaging.inbound.process";

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

        var transport = context.Transport;
        var destination = transport.Source;

        var baseTags = new TagList
        {
            { MessagingSemanticConventions.MessagingSystem, transport.MessagingSystem },
            { MessagingSemanticConventions.MessagingOperationName, MessagingSemanticConventions.ProcessOperation },
            { MessagingSemanticConventions.MessagingDestinationName, destination }
        };

        var traceContext = TraceContextHeaders.Extract(transport);
        using var activity = InboundDiagnostics.ActivitySource.StartActivity(
            ProcessActivityName,
            ActivityKind.Consumer,
            traceContext.TraceParent
        );

        if (activity is not null)
        {
            activity.DisplayName = $"{MessagingSemanticConventions.ProcessOperation} {destination}";
            activity.TraceStateString = traceContext.TraceState;
            foreach (var baggage in traceContext.Baggage)
            {
                activity.AddBaggage(baggage.Key, baggage.Value);
            }

            activity.SetTag(MessagingSemanticConventions.MessagingSystem, transport.MessagingSystem);
            activity.SetTag(
                MessagingSemanticConventions.MessagingOperationType,
                MessagingSemanticConventions.ProcessOperation
            );
            activity.SetTag(
                MessagingSemanticConventions.MessagingOperationName,
                MessagingSemanticConventions.ProcessOperation
            );
            activity.SetTag(MessagingSemanticConventions.MessagingDestinationName, destination);

            if (transport.MessageId is not null)
            {
                activity.SetTag(MessagingSemanticConventions.MessagingMessageId, transport.MessageId);
            }

            activity.SetTag(MessagingSemanticConventions.MessagingMessageBodySize, transport.Body.Length);

            if (!string.IsNullOrEmpty(transport.DestinationRoutingKey))
            {
                activity.SetTag(
                    MessagingSemanticConventions.MessagingRabbitMqDestinationRoutingKey,
                    transport.DestinationRoutingKey
                );
            }
        }

        string? errorType = null;
        var startedTimestamp = Stopwatch.GetTimestamp();
        try
        {
            await next(context).ConfigureAwait(false);
            activity?.SetStatus(ActivityStatusCode.Ok);
        }
        catch (OperationCanceledException) when (context.CancellationToken.IsCancellationRequested)
        {
            // A graceful-shutdown cancellation is not an error: no error.type is set on the span or the metric.
            throw;
        }
        catch (Exception exception)
        {
            errorType = MessagingSemanticConventions.ResolveErrorType(exception);
            activity?.SetTag(MessagingSemanticConventions.ErrorType, errorType);
            activity?.SetStatus(ActivityStatusCode.Error);
            RecordException(activity, exception);
            throw;
        }
        finally
        {
            var outcomeTags = BuildOutcomeTags(baseTags, errorType);
            InboundDiagnostics.ConsumedMessages.Add(1, outcomeTags);
            InboundDiagnostics.OperationDuration.Record(GetDurationSeconds(startedTimestamp), outcomeTags);
        }
    }

    private static TagList BuildOutcomeTags(TagList baseTags, string? errorType)
    {
        var tags = baseTags;
        if (errorType is not null)
        {
            tags.Add(MessagingSemanticConventions.ErrorType, errorType);
        }

        return tags;
    }

    private static double GetDurationSeconds(long startedTimestamp)
    {
        var elapsedTicks = Stopwatch.GetTimestamp() - startedTimestamp;
        return (double) elapsedTicks / Stopwatch.Frequency;
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
