using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Errors;
using Usf.Core.Messaging.Serialization;
using Usf.Transport.RabbitMq.Tests.TestSupport;
using Xunit;

namespace Usf.Transport.RabbitMq.Tests.Unit;

public sealed class RabbitMqChannelPoolingTests
{
    [Fact]
    public void RabbitMqMessagePublishingBuilder_UsesExpectedChannelPoolingDefaults()
    {
        var builder = new RabbitMqMessagePublishingBuilder();

        builder.UseConnectionFactory(static _ => new ConnectionFactory());

        var configuration = builder.Build();

        configuration.ChannelPoolingMode.Should().Be(RabbitMqChannelPoolingMode.PerTarget);
        configuration.MaxChannelsPerTarget.Should().Be(1);
        configuration.SharedChannelPoolSize.Should().Be(8);
    }

    [Fact]
    public async Task RabbitMqChannelPool_ReusesHealthyChannelsSequentially()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var createdChannels = new List<TestRabbitMqChannel>();
        await using var pool = new DefaultRabbitMqChannelPool(
            1,
            _ =>
            {
                var channel = new TestRabbitMqChannel();
                createdChannels.Add(channel);
                return Task.FromResult(channel.Object);
            }
        );

        IChannel firstChannel;
        await using (var lease = await pool.AcquireAsync(cancellationToken))
        {
            firstChannel = lease.Channel;
        }

        await using var secondLease = await pool.AcquireAsync(cancellationToken);

        secondLease.Channel.Should().BeSameAs(firstChannel);
        createdChannels.Should().HaveCount(1);
    }

    [Fact]
    public async Task RabbitMqChannelPool_WaitsForReturnedChannelsWhenBoundIsReached()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var channel = new TestRabbitMqChannel();
        await using var pool = new DefaultRabbitMqChannelPool(1, _ => Task.FromResult(channel.Object));

        var firstLease = await pool.AcquireAsync(cancellationToken);
        var waitingAcquire = pool.AcquireAsync(cancellationToken).AsTask();

        waitingAcquire.IsCompleted.Should().BeFalse();

        await firstLease.DisposeAsync();
        await using var secondLease = await waitingAcquire.WaitAsync(TimeSpan.FromSeconds(2), cancellationToken);

        secondLease.Channel.Should().BeSameAs(channel.Object);
    }

    [Fact]
    public async Task RabbitMqChannelPool_AcquiresDistinctChannelsForConcurrentLeases()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var createdChannels = new List<TestRabbitMqChannel>();
        await using var pool = new DefaultRabbitMqChannelPool(
            2,
            _ =>
            {
                var channel = new TestRabbitMqChannel();
                createdChannels.Add(channel);
                return Task.FromResult(channel.Object);
            }
        );

        var firstLeaseTask = pool.AcquireAsync(cancellationToken).AsTask();
        var secondLeaseTask = pool.AcquireAsync(cancellationToken).AsTask();
        var leases = await Task.WhenAll(firstLeaseTask, secondLeaseTask);

        try
        {
            leases[0].Channel.Should().NotBeSameAs(leases[1].Channel);
            createdChannels.Should().HaveCount(2);
        }
        finally
        {
            await leases[0].DisposeAsync();
            await leases[1].DisposeAsync();
        }
    }

    [Fact]
    public async Task RabbitMqChannelPool_ReplacesChannelsThatFaultWhileLeased()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var firstChannel = new TestRabbitMqChannel();
        var secondChannel = new TestRabbitMqChannel();
        var channels = new Queue<TestRabbitMqChannel>([firstChannel, secondChannel]);
        await using var pool = new DefaultRabbitMqChannelPool(1, _ => Task.FromResult(channels.Dequeue().Object));

        IChannel leasedChannel;
        var lease = await pool.AcquireAsync(cancellationToken);
        leasedChannel = lease.Channel;
        await firstChannel.ShutdownAsync();
        await lease.DisposeAsync();

        await using var replacementLease = await pool.AcquireAsync(cancellationToken);

        replacementLease.Channel.Should().NotBeSameAs(leasedChannel);
        replacementLease.Channel.Should().BeSameAs(secondChannel.Object);
        firstChannel.DisposeAsyncCallCount.Should().Be(1);
    }

    [Fact]
    public async Task RabbitMqTarget_ReusesChannelWhenPublishFailsButChannelStaysOpen()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var channel = new TestRabbitMqChannel();
        var firstAttempt = true;
        channel.BasicPublishAsyncHandler = () =>
        {
            if (firstAttempt)
            {
                firstAttempt = false;
                throw new InvalidOperationException("Broker rejected message.");
            }

            return default;
        };

        await using var pool = new DefaultRabbitMqChannelPool(1, _ => Task.FromResult(channel.Object));
        var target = new RabbitMqFanoutTarget<ValidationMessageA>(
            "target",
            new Utf8JsonMessageSerializer(),
            pool,
            false,
            "exchange",
            false
        );

        Func<Task> firstPublish = async () =>
            await target.PublishAsync(new ValidationMessageA("first"), cancellationToken);

        await firstPublish.Should().ThrowAsync<InvalidOperationException>();
        await target.PublishAsync(new ValidationMessageA("second"), cancellationToken);

        channel.BasicPublishCallCount.Should().Be(2);
        channel.DisposeAsyncCallCount.Should().Be(0);
    }

    [Fact]
    public async Task RabbitMqTarget_ReleasesLeaseSlotWhenPublishFaultsAndClosesChannel()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var firstChannel = new TestRabbitMqChannel();
        var secondChannel = new TestRabbitMqChannel();
        var channels = new Queue<TestRabbitMqChannel>([firstChannel, secondChannel]);
        firstChannel.BasicPublishAsyncHandler = async () =>
        {
            await firstChannel.ShutdownAsync().ConfigureAwait(false);
            throw new InvalidOperationException("Publish failed.");
        };

        await using var pool = new DefaultRabbitMqChannelPool(1, _ => Task.FromResult(channels.Dequeue().Object));
        var target = new RabbitMqFanoutTarget<ValidationMessageA>(
            "target",
            new Utf8JsonMessageSerializer(),
            pool,
            false,
            "exchange",
            false
        );

        Func<Task> firstPublish = async () =>
            await target.PublishAsync(new ValidationMessageA("first"), cancellationToken);

        await firstPublish.Should().ThrowAsync<InvalidOperationException>();
        await target.PublishAsync(new ValidationMessageA("second"), cancellationToken);

        firstChannel.DisposeAsyncCallCount.Should().Be(1);
        secondChannel.BasicPublishCallCount.Should().Be(1);
    }

    [Fact]
    public async Task RabbitMqConnectionManager_ThrowsWhenWorstCaseChannelCountExceedsBrokerLimit()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var builder = new RabbitMqMessagePublishingBuilder();
        builder.UseConnectionFactory(static _ => new ConnectionFactory());
        builder.UseMaxChannelsPerTarget(2);
        builder.Exchange("orders", ExchangeType.Fanout);
        builder.Publish<ValidationMessageA>(
            route => route.ToFanoutExchange("orders").WithSerializer<Utf8JsonMessageSerializer>()
        );
        builder.PublishNamed<ValidationMessageA>(
            "secondary",
            route => route.ToFanoutExchange("orders").WithSerializer<Utf8JsonMessageSerializer>()
        );

        var configuration = builder.Build();
        var connection = new TestRabbitMqConnection
        {
            ChannelMax = 3
        };
        var connectionManager = new RabbitMqConnectionManager(
            configuration,
            _ => Task.FromResult(connection.Object)
        );

        Func<Task> action = async () => await connectionManager.GetConnectionAsync(cancellationToken);

        var exception = await action.Should().ThrowAsync<MessageTopologyValidationException>();
        exception.Which.ValidationErrors.Should().ContainSingle();
        exception.Which.ValidationErrors[0].Should()
           .Be("RabbitMQ publish topology may open up to 4 channels (PerTarget mode, 2 targets × max 2), but the broker negotiated channel_max=3.");
        connection.DisposeAsyncCallCount.Should().Be(1);
    }

    [Fact]
    public void RabbitMqMessageTopologyCompiler_LogsWorstCaseChannelCountAtCompileTime()
    {
        var loggerProvider = new RecordingLoggerProvider();
        using var loggerFactory = new RecordingLoggerFactory(loggerProvider);
        var services = new ServiceCollection();
        services.AddSingleton<ILoggerFactory>(loggerFactory);
        services.AddSingleton<Utf8JsonMessageSerializer>();
        services.AddRabbitMqMessagePublishing(
            builder =>
            {
                builder.UseConnectionFactory(static _ => new ConnectionFactory());
                builder.UseChannelPoolingMode(RabbitMqChannelPoolingMode.Shared);
                builder.UseSharedChannelPoolSize(11);
                builder.Exchange("orders", ExchangeType.Fanout);
                builder.Publish<ValidationMessageA>(
                    route => route.ToFanoutExchange("orders").WithSerializer<Utf8JsonMessageSerializer>()
                );
            }
        );
        using var serviceProvider = services.BuildServiceProvider();

        _ = serviceProvider.GetRequiredService<RabbitMqCompiledTopology>();

        loggerProvider.Entries.Should().Contain(
            entry => entry.LogLevel == LogLevel.Information &&
                     entry.Message ==
                     "RabbitMQ publish topology may open up to 11 channels (Shared mode, shared pool size 11)."
        );
    }

    [Fact]
    public async Task RabbitMqCompiledTopology_DisposesTargetsBeforeSharedPool()
    {
        var disposalEvents = new List<string>();
        var sharedPool = new TrackingChannelPool("shared-pool", disposalEvents);
        var firstTarget = new TrackingTarget("target-a", disposalEvents);
        var secondTarget = new TrackingTarget("target-b", disposalEvents);
        var topology = new RabbitMqCompiledTopology(
            new MessageTopology(new Dictionary<Type, Target>(), new Dictionary<string, Target>(StringComparer.Ordinal)),
            Array.Empty<Usf.Transport.RabbitMq.Configuration.RabbitMqExchangeDefinition>(),
            Array.Empty<Usf.Transport.RabbitMq.Configuration.RabbitMqQueueDefinition>(),
            Array.Empty<Usf.Transport.RabbitMq.Configuration.RabbitMqBindingDefinition>(),
            [firstTarget, secondTarget],
            sharedPool
        );

        await topology.DisposeAsync();

        disposalEvents.Should().Equal("target-a", "target-b", "shared-pool");
    }
}
