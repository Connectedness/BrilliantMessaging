using System;
using System.Linq;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;
using BrilliantMessaging.Core.Messaging.Outbound;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace BrilliantMessaging.Transport.InMemory;

/// <summary>
/// Provides the <c>AddInMemory*Topology</c> extension methods that register in-memory topologies (unified,
/// publish-only, or consume-only) onto a <see cref="BrilliantMessagingBuilder" />. The in-memory transport is
/// process-local and non-durable: its state lives in a service-provider-scoped <see cref="InMemoryBroker" /> and
/// never crosses a process boundary.
/// </summary>
public static class InMemoryTransportModule
{
    /// <summary>
    /// The default name used by the parameterless inbound topology registration overload.
    /// It deliberately differs from <see cref="Topology.DefaultName" /> so that an outbound topology and an inbound
    /// topology registered without explicit names do not collide.
    /// </summary>
    public const string DefaultInboundName = "default-inbound";

    /// <summary>
    /// Registers a unified in-memory topology that may contain both outbound targets and inbound consumers, sharing
    /// a single broker between publishers and consumers.
    /// </summary>
    /// <param name="builder">The BrilliantMessaging builder.</param>
    /// <param name="configure">A callback that configures the topology.</param>
    /// <returns>The same builder for chaining.</returns>
    public static BrilliantMessagingBuilder AddInMemoryTopology(
        this BrilliantMessagingBuilder builder,
        Action<InMemoryTopologyBuilder> configure
    )
    {
        return builder.AddInMemoryTopology(Topology.DefaultName, configure);
    }

    /// <inheritdoc cref="AddInMemoryTopology(BrilliantMessagingBuilder, Action{InMemoryTopologyBuilder})" />
    /// <param name="builder">The BrilliantMessaging builder.</param>
    /// <param name="topologyName">The topology name.</param>
    /// <param name="configure">A callback that configures the topology.</param>
    public static BrilliantMessagingBuilder AddInMemoryTopology(
        this BrilliantMessagingBuilder builder,
        string topologyName,
        Action<InMemoryTopologyBuilder> configure
    )
    {
        return AddInMemoryTopologyCore(builder, topologyName, configure);
    }

    /// <summary>
    /// Registers a publish-only in-memory topology.
    /// </summary>
    /// <param name="builder">The BrilliantMessaging builder.</param>
    /// <param name="configure">A callback that configures the outbound topology.</param>
    /// <returns>The same builder for chaining.</returns>
    public static BrilliantMessagingBuilder AddInMemoryOutboundTopology(
        this BrilliantMessagingBuilder builder,
        Action<IInMemoryOutboundTopologyBuilder> configure
    )
    {
        return builder.AddInMemoryOutboundTopology(Topology.DefaultName, configure);
    }

    /// <inheritdoc cref="AddInMemoryOutboundTopology(BrilliantMessagingBuilder, Action{IInMemoryOutboundTopologyBuilder})" />
    /// <param name="builder">The BrilliantMessaging builder.</param>
    /// <param name="topologyName">The topology name.</param>
    /// <param name="configure">A callback that configures the outbound topology.</param>
    public static BrilliantMessagingBuilder AddInMemoryOutboundTopology(
        this BrilliantMessagingBuilder builder,
        string topologyName,
        Action<IInMemoryOutboundTopologyBuilder> configure
    )
    {
        if (configure is null)
        {
            throw new ArgumentNullException(nameof(configure));
        }

        return AddInMemoryTopologyCore(builder, topologyName, topologyBuilder => configure(topologyBuilder));
    }

    /// <summary>
    /// Registers a consume-only in-memory topology. The topology name defaults to <see cref="DefaultInboundName" />
    /// so it can be paired with an outbound topology that defaults to <see cref="Topology.DefaultName" />.
    /// </summary>
    /// <param name="builder">The BrilliantMessaging builder.</param>
    /// <param name="configure">A callback that configures the inbound topology.</param>
    /// <returns>The same builder for chaining.</returns>
    public static BrilliantMessagingBuilder AddInMemoryInboundTopology(
        this BrilliantMessagingBuilder builder,
        Action<IInMemoryInboundTopologyBuilder> configure
    )
    {
        return builder.AddInMemoryInboundTopology(DefaultInboundName, configure);
    }

    /// <inheritdoc cref="AddInMemoryInboundTopology(BrilliantMessagingBuilder, Action{IInMemoryInboundTopologyBuilder})" />
    /// <param name="builder">The BrilliantMessaging builder.</param>
    /// <param name="topologyName">The topology name.</param>
    /// <param name="configure">A callback that configures the inbound topology.</param>
    public static BrilliantMessagingBuilder AddInMemoryInboundTopology(
        this BrilliantMessagingBuilder builder,
        string topologyName,
        Action<IInMemoryInboundTopologyBuilder> configure
    )
    {
        if (configure is null)
        {
            throw new ArgumentNullException(nameof(configure));
        }

        return AddInMemoryTopologyCore(builder, topologyName, topologyBuilder => configure(topologyBuilder));
    }

    private static BrilliantMessagingBuilder AddInMemoryTopologyCore(
        BrilliantMessagingBuilder builder,
        string topologyName,
        Action<InMemoryTopologyBuilder> configure
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

        if (string.IsNullOrWhiteSpace(topologyName))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(topologyName));
        }

        var topologyBuilder = new InMemoryTopologyBuilder();
        configure(topologyBuilder);
        var configuration = ((IBuildable<InMemoryTopologyConfiguration>) topologyBuilder).Build();

        var services = builder.Services;
        builder.Topologies.Add(topologyName);

        foreach (var handler in configuration.Consumers.SelectMany(static consumer => consumer.Handlers))
        {
            services.TryAddScoped(handler.HandlerType);
        }

        services.TryAddSingleton<IInMemoryDelayScheduler, RealTimeInMemoryDelayScheduler>();

        services.AddKeyedSingleton<InMemoryTopology>(
            topologyName,
            (serviceProvider, _) =>
            {
                var loggerFactory = serviceProvider.GetService<ILoggerFactory>() ?? NullLoggerFactory.Instance;
                var compiler = new InMemoryTopologyCompiler(
                    serviceProvider.GetRequiredService<IMessageContractRegistry>(),
                    serviceProvider.GetRequiredService<IMessageSerializer>(),
                    serializerType => (IMessageSerializer?) serviceProvider.GetService(serializerType),
                    serviceProvider.GetRequiredService<IServiceScopeFactory>(),
                    serviceProvider.GetRequiredService<IInMemoryDelayScheduler>(),
                    loggerFactory
                );
                return compiler.Compile(topologyName, configuration);
            }
        );
        services.AddKeyedSingleton<Topology>(
            topologyName,
            (serviceProvider, key) => serviceProvider.GetRequiredKeyedService<InMemoryTopology>(key)
        );
        services.AddKeyedSingleton<InMemoryBroker>(
            topologyName,
            (serviceProvider, key) => serviceProvider.GetRequiredKeyedService<InMemoryTopology>(key).Broker
        );

        if (string.Equals(topologyName, Topology.DefaultName, StringComparison.Ordinal))
        {
            services.TryAddSingleton(
                serviceProvider => serviceProvider.GetRequiredKeyedService<InMemoryTopology>(Topology.DefaultName)
            );
            services.TryAddSingleton(
                serviceProvider => serviceProvider.GetRequiredKeyedService<InMemoryBroker>(Topology.DefaultName)
            );
        }

        if (configuration.HasInboundEndpoints)
        {
            services.AddSingleton<ITopologyRuntime>(
                serviceProvider => new InMemoryTopologyRuntime(
                    serviceProvider.GetRequiredKeyedService<InMemoryTopology>(topologyName)
                )
            );
        }

        return builder;
    }
}
