using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;
using BrilliantMessaging.Core.Messaging.Outbound;
using BrilliantMessaging.Transport.Nats.Inbound;
using BrilliantMessaging.Transport.Nats.Outbound;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;

namespace BrilliantMessaging.Transport.Nats;

/// <summary>
/// Provides <c>AddNats*Topology</c> extension methods for JetStream-backed NATS messaging. Core NATS pub/sub is
/// not implemented by this package.
/// </summary>
public static class NatsTransportModule
{
    /// <summary>
    /// Registers a unified JetStream-backed NATS topology.
    /// </summary>
    public static BrilliantMessagingBuilder AddNatsTopology(
        this BrilliantMessagingBuilder builder,
        Action<NatsTopologyBuilder> configure
    )
    {
        return builder.AddNatsTopology(Topology.DefaultName, configure);
    }

    /// <summary>
    /// Registers a named unified JetStream-backed NATS topology.
    /// </summary>
    public static BrilliantMessagingBuilder AddNatsTopology(
        this BrilliantMessagingBuilder builder,
        string topologyName,
        Action<NatsTopologyBuilder> configure
    )
    {
        if (builder is null)
        {
            throw new ArgumentNullException(nameof(builder));
        }

        if (configure is null)
        {
            throw new ArgumentNullException(nameof(configure));
        }

        NatsTopologyBuilder topologyBuilder = new ();
        configure(topologyBuilder);
        return AddNatsTopologyCore(
            builder,
            topologyName,
            ((IBuildable<NatsTopologyConfiguration>) topologyBuilder).Build()
        );
    }

    /// <summary>
    /// Registers a publish-only JetStream-backed NATS topology.
    /// </summary>
    public static BrilliantMessagingBuilder AddNatsOutboundTopology(
        this BrilliantMessagingBuilder builder,
        Action<INatsOutboundTopologyBuilder> configure
    )
    {
        return builder.AddNatsOutboundTopology(Topology.DefaultName, configure);
    }

    /// <summary>
    /// Registers a named publish-only JetStream-backed NATS topology.
    /// </summary>
    public static BrilliantMessagingBuilder AddNatsOutboundTopology(
        this BrilliantMessagingBuilder builder,
        string topologyName,
        Action<INatsOutboundTopologyBuilder> configure
    )
    {
        if (builder is null)
        {
            throw new ArgumentNullException(nameof(builder));
        }

        if (configure is null)
        {
            throw new ArgumentNullException(nameof(configure));
        }

        NatsTopologyBuilder topologyBuilder = new ();
        configure(topologyBuilder);
        return AddNatsTopologyCore(
            builder,
            topologyName,
            ((IBuildable<NatsTopologyConfiguration>) topologyBuilder).Build()
        );
    }

    /// <summary>
    /// Registers a consume-only JetStream-backed NATS topology. The default name does not collide with the
    /// default outbound topology name.
    /// </summary>
    public static BrilliantMessagingBuilder AddNatsInboundTopology(
        this BrilliantMessagingBuilder builder,
        Action<INatsInboundTopologyBuilder> configure
    )
    {
        return builder.AddNatsInboundTopology(NatsTopology.DefaultInboundName, configure);
    }

    /// <summary>
    /// Registers a named consume-only JetStream-backed NATS topology.
    /// </summary>
    public static BrilliantMessagingBuilder AddNatsInboundTopology(
        this BrilliantMessagingBuilder builder,
        string topologyName,
        Action<INatsInboundTopologyBuilder> configure
    )
    {
        if (builder is null)
        {
            throw new ArgumentNullException(nameof(builder));
        }

        if (configure is null)
        {
            throw new ArgumentNullException(nameof(configure));
        }

        NatsTopologyBuilder topologyBuilder = new ();
        configure(topologyBuilder);
        return AddNatsTopologyCore(
            builder,
            topologyName,
            ((IBuildable<NatsTopologyConfiguration>) topologyBuilder).Build()
        );
    }

    private static BrilliantMessagingBuilder AddNatsTopologyCore(
        BrilliantMessagingBuilder builder,
        string topologyName,
        NatsTopologyConfiguration configuration
    )
    {
        var services = builder.Services;
        builder.Topologies.Add(topologyName);

        foreach (var handler in configuration.Consumers.SelectMany(static consumer => consumer.Handlers))
        {
            services.TryAddScoped(handler.HandlerType);
        }

        services.AddKeyedSingleton<NatsTopology>(
            topologyName,
            (serviceProvider, _) =>
            {
                NatsConnectionProvider connectionProvider = new (
                    cancellationToken => CreateOptionsAsync(configuration, serviceProvider, cancellationToken)
                );
                NatsTopologyCompiler compiler = new (
                    serviceProvider.GetRequiredService<IMessageContractRegistry>(),
                    serviceProvider.GetRequiredService<IMessageSerializer>(),
                    serializerType => (IMessageSerializer?) serviceProvider.GetService(serializerType),
                    serviceType => IsServiceRegistered(serviceProvider, serviceType)
                );
                return compiler.Compile(topologyName, configuration, connectionProvider);
            }
        );
        services.AddKeyedSingleton<Topology>(
            topologyName,
            (serviceProvider, key) => serviceProvider.GetRequiredKeyedService<NatsTopology>(key)
        );
        if (string.Equals(topologyName, Topology.DefaultName, StringComparison.Ordinal))
        {
            services.TryAddSingleton<NatsTopology>(
                serviceProvider => serviceProvider.GetRequiredKeyedService<NatsTopology>(topologyName)
            );
        }

        services.AddSingleton<ITopologyProvisioner>(
            serviceProvider => new NatsTopologyProvisioner(
                serviceProvider.GetRequiredKeyedService<NatsTopology>(topologyName)
            )
        );

        if (configuration.HasInboundEndpoints)
        {
            services.AddSingleton<ITopologyRuntime>(
                serviceProvider => new NatsTopologyRuntime(
                    serviceProvider.GetRequiredKeyedService<NatsTopology>(topologyName),
                    serviceProvider.GetRequiredService<IServiceScopeFactory>(),
                    serviceProvider.GetService<ILogger<NatsTopologyRuntime>>()
                )
            );
        }

        return builder;
    }

    private static Task<NatsOpts> CreateOptionsAsync(
        NatsTopologyConfiguration configuration,
        IServiceProvider serviceProvider,
        CancellationToken cancellationToken
    )
    {
        cancellationToken.ThrowIfCancellationRequested();
        var factory = configuration.CreateOptions ??
                      throw new TopologyValidationException(["NATS connection options must be configured."]);
        var options = factory(serviceProvider);
        if (options is null)
        {
            throw new TopologyValidationException(["NATS connection options factory returned null."]);
        }

        return Task.FromResult(options);
    }

    private static bool IsServiceRegistered(IServiceProvider serviceProvider, Type serviceType)
    {
        return serviceProvider.GetService(serviceType) is not null;
    }
}
