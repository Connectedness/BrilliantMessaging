using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;
using BrilliantMessaging.Core.Messaging.Outbound;
using Microsoft.Extensions.DependencyInjection;

namespace BrilliantMessaging.Transport.InMemory;

/// <summary>
/// A small sociable-test harness that wires BrilliantMessaging with an in-memory topology, exposes the publisher
/// and broker, and drives the topology runtimes through the same start/stop seam the host uses.
/// </summary>
public sealed class InMemoryTestHost : IAsyncDisposable
{
    private readonly ITopologyRuntime[] _runtimes;
    private readonly ServiceProvider _serviceProvider;

    private InMemoryTestHost(ServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider;
        Probe = serviceProvider.GetService<HandlerProbe>();
        _runtimes = serviceProvider.GetServices<ITopologyRuntime>().ToArray();
        Publisher = serviceProvider.GetRequiredService<IMessagePublisher>();
    }

    /// <summary>
    /// Gets the message publisher.
    /// </summary>
    public IMessagePublisher Publisher { get; }

    /// <summary>
    /// Gets the <see cref="HandlerProbe" /> registered with the host, or <see langword="null" /> when none was
    /// registered. Register one (for example with <c>services.AddSingleton&lt;HandlerProbe&gt;()</c>) and resolve
    /// it into your handlers to record invocations.
    /// </summary>
    public HandlerProbe? Probe { get; }

    /// <summary>
    /// Resolves the default in-memory broker.
    /// </summary>
    public InMemoryBroker Broker => _serviceProvider.GetRequiredService<InMemoryBroker>();

    /// <summary>
    /// Resolves the default topology compiled for the host.
    /// </summary>
    public Topology Topology => _serviceProvider.GetRequiredService<Topology>();

    /// <summary>
    /// Stops the topology runtimes and disposes the underlying service provider.
    /// </summary>
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
    /// Builds and starts a host. The supplied configuration runs against a fresh
    /// <see cref="BrilliantMessagingBuilder" />.
    /// </summary>
    /// <param name="configure">Configures the messaging builder (contracts, topologies, handlers).</param>
    /// <param name="scheduler">An optional delay scheduler registered before the topology is built.</param>
    /// <param name="source">The CloudEvents source applied to published messages.</param>
    public static async Task<InMemoryTestHost> StartAsync(
        Action<BrilliantMessagingBuilder> configure,
        IInMemoryDelayScheduler? scheduler = null,
        string source = "/in-memory"
    )
    {
        var services = new ServiceCollection();
        if (scheduler is not null)
        {
            services.AddSingleton(scheduler);
        }

        var builder = services
           .AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = source);
        configure(builder);

        var serviceProvider = services.BuildServiceProvider();
        var host = new InMemoryTestHost(serviceProvider);

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
        await StopRuntimesAsync(CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>
    /// Stops the topology runtimes with a caller-supplied cancellation token.
    /// </summary>
    public async Task StopRuntimesAsync(CancellationToken cancellationToken)
    {
        for (var index = _runtimes.Length - 1; index >= 0; index--)
        {
            await _runtimes[index].StopAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
