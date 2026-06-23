using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Tests.Messaging.TestSupport;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace BrilliantMessaging.Core.Tests.Messaging;

public sealed class TopologyRegistryTests
{
    [Fact]
    public void GetRequiredTopology_ResolvesRegisteredKeyedTopology()
    {
        var topology = EmptyTopology.Create();
        TopologyRegistrationCatalog catalog = new ();
        catalog.Add(Topology.DefaultName);
        TestKeyedServiceProvider services = new ();
        services.Add(Topology.DefaultName, topology);
        TopologyRegistry registry = new (services, catalog);

        registry.Names.Should().ContainSingle().Which.Should().Be(Topology.DefaultName);
        registry.TryGetTopology(Topology.DefaultName, out var resolved).Should().BeTrue();
        resolved.Should().BeSameAs(topology);
        registry.GetRequiredTopology(Topology.DefaultName).Should().BeSameAs(topology);
    }

    [Fact]
    public void GetRequiredTopology_ReportsUnknownTopologyWithRegisteredNames()
    {
        TopologyRegistrationCatalog catalog = new ();
        catalog.Add("orders");
        catalog.Add("billing");
        TestKeyedServiceProvider services = new ();
        TopologyRegistry registry = new (services, catalog);

        var tryGet = registry.TryGetTopology("missing", out var topology);
        var getRequired = () => registry.GetRequiredTopology("missing");

        tryGet.Should().BeFalse();
        topology.Should().BeNull();
        getRequired.Should().Throw<InvalidOperationException>()
           .WithMessage("Topology 'missing' is not registered. Registered topologies: billing, orders.");
    }

    [Fact]
    public void Constructor_RejectsNullArguments()
    {
        TopologyRegistrationCatalog catalog = new ();
        TestKeyedServiceProvider services = new ();

        var nullServices = () => new TopologyRegistry(null!, catalog);
        var nullCatalog = () => new TopologyRegistry(services, null!);

        nullServices.Should().Throw<ArgumentNullException>().WithParameterName("serviceProvider");
        nullCatalog.Should().Throw<ArgumentNullException>().WithParameterName("catalog");
    }

    private sealed class TestKeyedServiceProvider : IServiceProvider, IKeyedServiceProvider
    {
        private readonly Dictionary<(Type ServiceType, object? Key), object> _services = new ();

        public void Add<TService>(object? key, TService service)
            where TService : notnull
        {
            _services[(typeof(TService), key)] = service;
        }

        public object? GetService(Type serviceType)
        {
            return null;
        }

        public object? GetKeyedService(Type serviceType, object? serviceKey)
        {
            return _services.TryGetValue((serviceType, serviceKey), out var service) ? service : null;
        }

        public object GetRequiredKeyedService(Type serviceType, object? serviceKey)
        {
            return GetKeyedService(serviceType, serviceKey) ??
                   throw new InvalidOperationException($"No keyed service is registered for '{serviceType}'.");
        }
    }
}

public sealed class TopologyProvisioningHostedServiceTests
{
    [Fact]
    public async Task StartAsync_ProvisionsEveryTopologyInRegistrationOrder()
    {
        var events = new List<string>();
        RecordingProvisioner first = new ("first", events);
        RecordingProvisioner second = new ("second", events);
        TopologyProvisioningHostedService hostedService = new ([first, second]);

        await hostedService.StartAsync(TestContext.Current.CancellationToken);
        await hostedService.StopAsync(TestContext.Current.CancellationToken);

        events.Should().Equal("first", "second");
        first.CancellationToken.Should().Be(TestContext.Current.CancellationToken);
        second.CancellationToken.Should().Be(TestContext.Current.CancellationToken);
    }

    [Fact]
    public void Constructor_RejectsNullProvisioners()
    {
        var act = () => new TopologyProvisioningHostedService(null!);

        act.Should().Throw<ArgumentNullException>().WithParameterName("topologyProvisioners");
    }

    private sealed class RecordingProvisioner : ITopologyProvisioner
    {
        private readonly List<string> _events;
        private readonly string _name;

        public RecordingProvisioner(string name, List<string> events)
        {
            _name = name;
            _events = events;
        }

        public CancellationToken CancellationToken { get; private set; }

        public Task ProvisionAsync(CancellationToken cancellationToken = default)
        {
            CancellationToken = cancellationToken;
            _events.Add(_name);
            return Task.CompletedTask;
        }
    }
}
