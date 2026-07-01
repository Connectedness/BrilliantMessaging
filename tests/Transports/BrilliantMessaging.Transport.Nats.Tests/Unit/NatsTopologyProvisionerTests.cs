using System;
using System.Collections.Generic;
using BrilliantMessaging.Transport.Nats.Inbound;
using FluentAssertions;
using NATS.Client.JetStream.Models;
using Xunit;

namespace BrilliantMessaging.Transport.Nats.Tests.Unit;

public sealed class NatsTopologyProvisionerTests
{
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
            null,
            new Dictionary<string, NatsInboundEndpoint>(StringComparer.Ordinal)
        );

        var config = NatsTopologyProvisioner.ToConsumerConfig(consumer);

        config.FilterSubject.Should().BeNull();
        config.DeliverPolicy.Should().Be(ConsumerConfigDeliverPolicy.All);
        config.AckPolicy.Should().Be(ConsumerConfigAckPolicy.Explicit);
    }
}
