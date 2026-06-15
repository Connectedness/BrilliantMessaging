using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Usf.Core.Messaging;
using Xunit;

namespace Usf.Core.Tests.Messaging;

public sealed class TopologyRuntimeHostedServiceTests
{
    [Fact]
    public async Task StartAsync_StartsEveryRuntime_AndStopsInReverseOrder()
    {
        var events = new List<string>();
        var first = new RecordingTopologyRuntime("first", events);
        var second = new RecordingTopologyRuntime("second", events);
        var hostedService = new TopologyRuntimeHostedService([first, second]);

        await hostedService.StartAsync(CancellationToken.None);
        await hostedService.StopAsync(CancellationToken.None);

        events.Should().Equal("start:first", "start:second", "stop:second", "stop:first");
    }

    [Fact]
    public void AddUsf_RegistersProvisioningHostedServiceBeforeRuntimeHostedService()
    {
        var services = new ServiceCollection();
        services.AddUsf();

        var hostedServiceImplementations = services
           .Where(static descriptor => descriptor.ServiceType == typeof(IHostedService))
           .Select(static descriptor => descriptor.ImplementationType)
           .ToList();

        var provisioningIndex = hostedServiceImplementations.IndexOf(typeof(TopologyProvisioningHostedService));
        var runtimeIndex = hostedServiceImplementations.IndexOf(typeof(TopologyRuntimeHostedService));

        provisioningIndex.Should().BeGreaterThanOrEqualTo(0);
        runtimeIndex.Should().BeGreaterThan(provisioningIndex);
    }

    [Fact]
    public async Task StopAsync_ContinuesStoppingRemainingRuntimes_WhenOneFails()
    {
        var events = new List<string>();
        var first = new RecordingTopologyRuntime("first", events);
        var second = new RecordingTopologyRuntime("second", events)
            { StopException = new InvalidOperationException("boom") };
        var third = new RecordingTopologyRuntime("third", events);
        var hostedService = new TopologyRuntimeHostedService([first, second, third]);

        var stop = async () => await hostedService.StopAsync(CancellationToken.None);

        await stop.Should().ThrowAsync<InvalidOperationException>();
        events.Should().Equal("stop:third", "stop:second", "stop:first");
    }

    [Fact]
    public async Task StopAsync_RethrowsOriginalException_WhenExactlyOneRuntimeFails()
    {
        var events = new List<string>();
        var failure = new InvalidOperationException("boom");
        var first = new RecordingTopologyRuntime("first", events);
        var second = new RecordingTopologyRuntime("second", events) { StopException = failure };
        var hostedService = new TopologyRuntimeHostedService([first, second]);

        var stop = async () => await hostedService.StopAsync(CancellationToken.None);

        var thrown = (await stop.Should().ThrowAsync<InvalidOperationException>()).Which;
        thrown.Should().BeSameAs(failure);
        thrown.StackTrace.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task StopAsync_ThrowsAggregateInStopOrder_WhenMultipleRuntimesFail()
    {
        var events = new List<string>();
        var firstFailure = new InvalidOperationException("first failure");
        var secondFailure = new InvalidOperationException("second failure");

        // Registered first..second, so they stop second-then-first. The aggregate must preserve that stop order.
        var first = new RecordingTopologyRuntime("first", events) { StopException = firstFailure };
        var second = new RecordingTopologyRuntime("second", events) { StopException = secondFailure };
        var hostedService = new TopologyRuntimeHostedService([first, second]);

        var stop = async () => await hostedService.StopAsync(CancellationToken.None);

        var aggregate = (await stop.Should().ThrowAsync<AggregateException>()).Which;
        aggregate.InnerExceptions.Should().HaveCount(2);
        aggregate.InnerExceptions[0].Should().BeSameAs(secondFailure);
        aggregate.InnerExceptions[1].Should().BeSameAs(firstFailure);
    }

    [Fact]
    public async Task StopAsync_CollectsCancellation_AndStillStopsRemainingRuntimes()
    {
        var events = new List<string>();
        var cancellation = new OperationCanceledException("cancelled");
        var first = new RecordingTopologyRuntime("first", events);
        var second = new RecordingTopologyRuntime("second", events) { StopException = cancellation };
        var hostedService = new TopologyRuntimeHostedService([first, second]);

        var stop = async () => await hostedService.StopAsync(CancellationToken.None);

        var thrown = (await stop.Should().ThrowAsync<OperationCanceledException>()).Which;
        thrown.Should().BeSameAs(cancellation);
        events.Should().Equal("stop:second", "stop:first");
    }

    [Fact]
    public async Task StopAsync_IncludesCancellationInAggregate_WhenOtherRuntimesAlsoFail()
    {
        var events = new List<string>();
        var cancellation = new OperationCanceledException("cancelled");
        var failure = new InvalidOperationException("boom");

        // first stops last, second stops first. second cancels, first fails.
        var first = new RecordingTopologyRuntime("first", events) { StopException = failure };
        var second = new RecordingTopologyRuntime("second", events) { StopException = cancellation };
        var hostedService = new TopologyRuntimeHostedService([first, second]);

        var stop = async () => await hostedService.StopAsync(CancellationToken.None);

        var aggregate = (await stop.Should().ThrowAsync<AggregateException>()).Which;
        aggregate.InnerExceptions.Should().HaveCount(2);
        aggregate.InnerExceptions[0].Should().BeSameAs(cancellation);
        aggregate.InnerExceptions[1].Should().BeSameAs(failure);
        events.Should().Equal("stop:second", "stop:first");
    }

    private sealed class RecordingTopologyRuntime : ITopologyRuntime
    {
        private readonly List<string> _events;
        private readonly string _name;

        public RecordingTopologyRuntime(string name, List<string> events)
        {
            _name = name;
            _events = events;
            TopologyName = name;
        }

        public Exception? StopException { get; init; }

        public string TopologyName { get; }

        public Task StartAsync(CancellationToken cancellationToken = default)
        {
            _events.Add($"start:{_name}");
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken = default)
        {
            _events.Add($"stop:{_name}");
            if (StopException is not null)
            {
                throw StopException;
            }

            return Task.CompletedTask;
        }
    }
}
