using System;
using BrilliantMessaging.Core.Messaging.Inbound;
using BrilliantMessaging.Core.Messaging.Outbound;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

namespace BrilliantMessaging.OpenTelemetry;

/// <summary>
/// One-line registration of Brilliant Messaging instrumentation with the OpenTelemetry SDK. These are the supported
/// wiring helpers: they subscribe the configured <c>TracerProvider</c>/<c>MeterProvider</c> to the framework's
/// <c>BrilliantMessaging.Outbound</c> and <c>BrilliantMessaging.Inbound</c> activity sources and meters, whose spans and instruments already carry
/// the OpenTelemetry <c>messaging.*</c> semantic conventions (see
/// <see cref="BrilliantMessaging.Core.Messaging.MessagingSemanticConventions" />).
/// </summary>
/// <remarks>
/// The package references only <c>OpenTelemetry.Api</c>, so it forces no SDK dependency on consumers and adds no
/// package reference to <c>BrilliantMessaging.Core</c> or the transports. The source and meter names are reused from
/// <see cref="OutboundDiagnostics" />/<see cref="InboundDiagnostics" /> rather than duplicated as string literals.
/// Even without this package, a user can call <c>AddSource("BrilliantMessaging.Outbound")</c>/<c>AddMeter("BrilliantMessaging.Outbound")</c>
/// directly; these methods are the discoverable, named convenience.
/// </remarks>
public static class BrilliantMessagingInstrumentationExtensions
{
    /// <summary>
    /// Registers Brilliant Messaging outbound and inbound activity sources (<c>BrilliantMessaging.Outbound</c> and <c>BrilliantMessaging.Inbound</c>) with the
    /// tracer provider so their producer and consumer spans are collected.
    /// </summary>
    /// <param name="builder">The tracer provider builder to configure.</param>
    /// <returns>The same <paramref name="builder" /> for chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="builder" /> is <see langword="null" />.</exception>
    public static TracerProviderBuilder AddBrilliantMessagingInstrumentation(this TracerProviderBuilder builder)
    {
        if (builder is null)
        {
            throw new ArgumentNullException(nameof(builder));
        }

        return builder.AddSource(OutboundDiagnostics.ActivitySourceName, InboundDiagnostics.ActivitySourceName);
    }

    /// <summary>
    /// Registers Brilliant Messaging outbound and inbound meters (<c>BrilliantMessaging.Outbound</c> and <c>BrilliantMessaging.Inbound</c>) with the meter
    /// provider so their <c>messaging.client.*</c> instruments are collected.
    /// </summary>
    /// <param name="builder">The meter provider builder to configure.</param>
    /// <returns>The same <paramref name="builder" /> for chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="builder" /> is <see langword="null" />.</exception>
    public static MeterProviderBuilder AddBrilliantMessagingInstrumentation(this MeterProviderBuilder builder)
    {
        if (builder is null)
        {
            throw new ArgumentNullException(nameof(builder));
        }

        return builder.AddMeter(OutboundDiagnostics.MeterName, InboundDiagnostics.MeterName);
    }
}
