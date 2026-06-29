using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;
using BrilliantMessaging.Core.Messaging.Outbound;
using Microsoft.Extensions.DependencyInjection;

namespace BrilliantMessaging.Transport.InMemory.Tests.TestSupport;

/// <summary>
/// A small sociable-test harness that wires BrilliantMessaging with an in-memory topology, exposes the publisher
/// and broker, and drives the topology runtimes through the same start/stop seam the host uses.
/// </summary>
public sealed class InMemoryTestHost : IAsyncDisposable
{
    private readonly ITopologyRuntime[] _runtimes;
    private readonly ServiceProvider _serviceProvider;

    private InMemoryTestHost(ServiceProvider serviceProvider, HandlerProbe probe)
    {
        _serviceProvider = serviceProvider;
        Probe = probe;
        _runtimes = serviceProvider.GetServices<ITopologyRuntime>().ToArray();
        Publisher = serviceProvider.GetRequiredService<IMessagePublisher>();
    }

    /// <summary>
    /// Gets the message publisher.
    /// </summary>
    public IMessagePublisher Publisher { get; }

    /// <summary>
    /// Gets the shared probe handlers record their invocations on.
    /// </summary>
    public HandlerProbe Probe { get; }

    /// <summary>
    /// Resolves the default in-memory broker.
    /// </summary>
    public InMemoryBroker Broker => _serviceProvider.GetRequiredService<InMemoryBroker>();

    public async ValueTask DisposeAsync()
    {
        await StopRuntimesAsync().ConfigureAwait(false);
        await _serviceProvider.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Resolves the in-memory broker registered for the named topology.
    /// </summary>
    public InMemoryBroker BrokerFor(string topologyName)
    {
        return _serviceProvider.GetRequiredKeyedService<InMemoryBroker>(topologyName);
    }

    /// <summary>
    /// Builds and starts a host. The handler probe is registered as a singleton, and the supplied configuration
    /// runs against a fresh <see cref="BrilliantMessagingBuilder" />.
    /// </summary>
    public static async Task<InMemoryTestHost> StartAsync(
        Action<BrilliantMessagingBuilder> configure,
        IInMemoryDelayScheduler? scheduler = null
    )
    {
        var probe = new HandlerProbe();
        var services = new ServiceCollection();
        services.AddSingleton(probe);
        if (scheduler is not null)
        {
            services.AddSingleton(scheduler);
            services.AddSingleton(scheduler);
        }

        var builder = services
           .AddBrilliantMessaging()
           .UseCloudEvents(static options => options.Source = "/in-memory-tests");
        configure(builder);

        var serviceProvider = services.BuildServiceProvider();
        var host = new InMemoryTestHost(serviceProvider, probe);

        foreach (var runtime in host._runtimes)
        {
            await runtime.StartAsync(CancellationToken.None).ConfigureAwait(false);
        }

        return host;
    }

    /// <summary>
    /// Stops the topology runtimes (draining and then cancelling in-flight work) without disposing the container,
    /// so a test can still resolve services and assert post-shutdown behaviour.
    /// </summary>
    public async Task StopRuntimesAsync()
    {
        for (var index = _runtimes.Length - 1; index >= 0; index--)
        {
            await _runtimes[index].StopAsync(CancellationToken.None).ConfigureAwait(false);
        }
    }
}
