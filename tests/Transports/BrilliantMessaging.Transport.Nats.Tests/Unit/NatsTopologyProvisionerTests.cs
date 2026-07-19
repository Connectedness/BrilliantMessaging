using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Transport.Nats.Inbound;
using BrilliantMessaging.Transport.Nats.Tests.TestSupport;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using NATS.Client.Core;
using NATS.Client.JetStream.Models;
using Xunit;

namespace BrilliantMessaging.Transport.Nats.Tests.Unit;

[Collection("Diagnostics")]
public sealed class NatsTopologyProvisionerTests
{
    private const string ProvisionActivityName = "brilliantmessaging.outbound.topology.provision";

    private const string TransportNameTagName = "brilliantmessaging.outbound.transport.name";

    private const string OutcomeTagName = "brilliantmessaging.outbound.outcome";

    [Fact]
    public async Task ProvisionAsync_WhenProvisioningFails_RecordsTopologyProvisioningDiagnostics()
    {
        ServiceCollection services = new ();
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .AddNatsTopology(topology => topology.UseOptions(_ => null!));
        await using var provider = services.BuildServiceProvider();
        using var recorder = new OutboundTopologyProvisioningDiagnosticsRecorder();

        var provisioner = provider.GetRequiredService<ITopologyProvisioner>();

        var act = () => provisioner.ProvisionAsync(TestContext.Current.CancellationToken);

        await act.Should().ThrowAsync<TopologyValidationException>();
        var activity = recorder.StartedActivities.Should().ContainSingle().Which;
        activity.OperationName.Should().Be(ProvisionActivityName);
        activity.Status.Should().Be(ActivityStatusCode.Error);
        activity.GetTagItem(TransportNameTagName).Should().Be(NatsTopology.TransportNameValue);
        activity.GetTagItem(OutcomeTagName).Should().Be("failure");
        recorder.Attempts.Should().ContainSingle().Which.Should().Contain(
            new KeyValuePair<string, object?>(TransportNameTagName, NatsTopology.TransportNameValue)
        );
        recorder.Failures.Should().ContainSingle().Which.Should().Contain(
            new KeyValuePair<string, object?>(OutcomeTagName, "failure")
        );
        recorder.Durations.Should().ContainSingle().Which.Should().Contain(
            new KeyValuePair<string, object?>(OutcomeTagName, "failure")
        );
    }

    [Fact]
    public async Task ProvisionAsync_WhenCallerTokenIsCancelled_RecordsCancelledOutcomeWithoutFailure()
    {
        ServiceCollection services = new ();
        services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests")
           .AddNatsTopology(topology => topology.UseOptions(_ => new NatsOpts()));
        await using var provider = services.BuildServiceProvider();
        using var recorder = new OutboundTopologyProvisioningDiagnosticsRecorder();
        using CancellationTokenSource cancellationTokenSource = new ();
        await cancellationTokenSource.CancelAsync();
        var provisioner = provider.GetRequiredService<ITopologyProvisioner>();

        // ReSharper disable once AccessToDisposedClosure
        var act = () => provisioner.ProvisionAsync(cancellationTokenSource.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
        var activity = recorder.StartedActivities.Should().ContainSingle().Which;
        activity.OperationName.Should().Be(ProvisionActivityName);
        activity.Status.Should().Be(ActivityStatusCode.Unset);
        activity.GetTagItem(TransportNameTagName).Should().Be(NatsTopology.TransportNameValue);
        activity.GetTagItem(OutcomeTagName).Should().Be("cancelled");
        recorder.Attempts.Should().ContainSingle();
        recorder.Failures.Should().BeEmpty();
        recorder.Durations.Should().ContainSingle().Which.Should().Contain(
            new KeyValuePair<string, object?>(OutcomeTagName, "cancelled")
        );
    }

    [Theory]
    [InlineData(NatsStreamStorage.File, StreamConfigStorage.File)]
    [InlineData(NatsStreamStorage.Memory, StreamConfigStorage.Memory)]
    public void ToStreamConfig_MapsStoragePolicy(NatsStreamStorage storage, StreamConfigStorage expected)
    {
        NatsStreamDefinition stream = new (
            "ORDERS",
            ["orders.*"],
            null,
            null,
            null,
            storage,
            NatsStreamRetention.Limits,
            1
        );

        var config = NatsTopologyProvisioner.ToStreamConfig(stream);

        config.Storage.Should().Be(expected);
    }

    [Theory]
    [InlineData(NatsStreamRetention.Limits, StreamConfigRetention.Limits)]
    [InlineData(NatsStreamRetention.Interest, StreamConfigRetention.Interest)]
    [InlineData(NatsStreamRetention.WorkQueue, StreamConfigRetention.Workqueue)]
    public void ToStreamConfig_MapsRetentionPolicy(NatsStreamRetention retention, StreamConfigRetention expected)
    {
        NatsStreamDefinition stream = new (
            "ORDERS",
            ["orders.*"],
            null,
            null,
            null,
            NatsStreamStorage.File,
            retention,
            1
        );

        var config = NatsTopologyProvisioner.ToStreamConfig(stream);

        config.Retention.Should().Be(expected);
    }

    [Fact]
    public void ToStreamConfig_MapsOptionalLimits()
    {
        NatsStreamDefinition stream = new (
            "ORDERS",
            ["orders.*"],
            TimeSpan.FromMinutes(2),
            TimeSpan.FromHours(1),
            4096,
            NatsStreamStorage.File,
            NatsStreamRetention.Limits,
            3
        );

        var config = NatsTopologyProvisioner.ToStreamConfig(stream);

        config.Name.Should().Be("ORDERS");
        config.Subjects.Should().Equal("orders.*");
        config.DuplicateWindow.Should().Be(TimeSpan.FromMinutes(2));
        config.MaxAge.Should().Be(TimeSpan.FromHours(1));
        config.MaxMsgSize.Should().Be(4096);
        config.NumReplicas.Should().Be(3);
    }

    [Fact]
    public void ToConsumerConfig_LeavesFilterSubjectUnsetWhenConsumerHasNoFilter()
    {
        NatsInboundConsumer consumer = new (
            "ORDERS",
            "orders-worker",
            null,
            1,
            TimeSpan.FromSeconds(30),
            5,
            1024,
            8,
            null,
            new Dictionary<string, NatsInboundEndpoint>(StringComparer.Ordinal)
        );

        var config = NatsTopologyProvisioner.ToConsumerConfig(consumer);

        config.FilterSubject.Should().BeNull();
        config.DeliverPolicy.Should().Be(ConsumerConfigDeliverPolicy.All);
        config.AckPolicy.Should().Be(ConsumerConfigAckPolicy.Explicit);
    }

    [Fact]
    public void ToConsumerConfig_ProvisionsDoubledMaxDeliverAsShutdownInterruptionHeadroom()
    {
        NatsInboundConsumer consumer = new (
            "ORDERS",
            "orders-worker",
            null,
            1,
            TimeSpan.FromSeconds(30),
            5,
            1024,
            8,
            null,
            new Dictionary<string, NatsInboundEndpoint>(StringComparer.Ordinal)
        );

        var config = NatsTopologyProvisioner.ToConsumerConfig(consumer);

        config.MaxDeliver.Should().Be(10);
    }
}
