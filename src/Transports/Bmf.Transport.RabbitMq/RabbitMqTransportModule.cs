using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using Bmf.Core.Messaging;
using Bmf.Core.Messaging.Inbound;
using Bmf.Core.Messaging.Outbound;

using Bmf.Transport.RabbitMq.Inbound;
using Bmf.Transport.RabbitMq.Outbound;

namespace Bmf.Transport.RabbitMq;

/// <summary>
/// Provides the <c>AddRabbitMq*Topology</c> extension methods that register RabbitMQ topologies (unified,
/// publish-only, or consume-only) onto a <see cref="BmfBuilder" />.
/// </summary>
public static class RabbitMqTransportModule
{
    /// <summary>
    /// Registers a unified RabbitMQ topology that may contain both outbound targets and inbound consumers and
    /// therefore shares a single connection between publishers and consumers. The RabbitMQ production checklist
    /// (https://www.rabbitmq.com/docs/production-checklist#apps-connection-management) recommends dedicated
    /// connections for publishing and consuming — when a publishing connection is throttled by broker flow
    /// control, a shared connection also stalls consumer acknowledgements, precisely when the broker needs
    /// consumers to drain queues. Prefer
    /// <see cref="AddRabbitMqOutboundTopology(BmfBuilder, Action{IRabbitMqOutboundTopologyBuilder})" /> plus
    /// <see cref="AddRabbitMqInboundTopology(BmfBuilder, Action{IRabbitMqInboundTopologyBuilder})" /> for
    /// production services; a single shared connection is appropriate for low-traffic services and tests.
    /// </summary>
    public static BmfBuilder AddRabbitMqTopology(
        this BmfBuilder builder,
        Action<RabbitMqTopologyBuilder> configure
    )
    {
        return builder.AddRabbitMqTopology(Topology.DefaultName, configure);
    }

    /// <inheritdoc cref="AddRabbitMqTopology(BmfBuilder, Action{RabbitMqTopologyBuilder})" />
    public static BmfBuilder AddRabbitMqTopology(
        this BmfBuilder builder,
        string topologyName,
        Action<RabbitMqTopologyBuilder> configure
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

        var topologyBuilder = new RabbitMqTopologyBuilder();
        configure(topologyBuilder);
        return AddRabbitMqTopologyCore(builder, topologyName, topologyBuilder.Build());
    }

    /// <summary>
    /// Registers a publish-only RabbitMQ topology with a dedicated connection, following the RabbitMQ
    /// production checklist recommendation to separate publishing and consuming connections
    /// (https://www.rabbitmq.com/docs/production-checklist#apps-connection-management): when a publishing
    /// connection is throttled by broker flow control, a shared connection would also stall consumer
    /// acknowledgements. Pair it with
    /// <see cref="AddRabbitMqInboundTopology(BmfBuilder, Action{IRabbitMqInboundTopologyBuilder})" /> for the
    /// consuming side; both default names (<see cref="Topology.DefaultName" /> and
    /// <see cref="RabbitMqTopology.DefaultInboundName" />) coexist without a collision. Use
    /// <see cref="AddRabbitMqTopology(BmfBuilder, Action{RabbitMqTopologyBuilder})" /> instead when a single
    /// shared connection is appropriate (low-traffic services, tests).
    /// </summary>
    public static BmfBuilder AddRabbitMqOutboundTopology(
        this BmfBuilder builder,
        Action<IRabbitMqOutboundTopologyBuilder> configure
    )
    {
        return builder.AddRabbitMqOutboundTopology(Topology.DefaultName, configure);
    }

    /// <inheritdoc cref="AddRabbitMqOutboundTopology(BmfBuilder, Action{IRabbitMqOutboundTopologyBuilder})" />
    public static BmfBuilder AddRabbitMqOutboundTopology(
        this BmfBuilder builder,
        string topologyName,
        Action<IRabbitMqOutboundTopologyBuilder> configure
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

        var topologyBuilder = new RabbitMqTopologyBuilder();
        configure(topologyBuilder);
        return AddRabbitMqTopologyCore(builder, topologyName, topologyBuilder.Build());
    }

    /// <summary>
    /// Registers a consume-only RabbitMQ topology with a dedicated connection, following the RabbitMQ
    /// production checklist recommendation to separate publishing and consuming connections
    /// (https://www.rabbitmq.com/docs/production-checklist#apps-connection-management): when a publishing
    /// connection is throttled by broker flow control, a shared connection would also stall consumer
    /// acknowledgements. The topology name defaults to <see cref="RabbitMqTopology.DefaultInboundName" /> so
    /// that it can be paired with
    /// <see cref="AddRabbitMqOutboundTopology(BmfBuilder, Action{IRabbitMqOutboundTopologyBuilder})" /> (which
    /// defaults to <see cref="Topology.DefaultName" />) without a collision. Use
    /// <see cref="AddRabbitMqTopology(BmfBuilder, Action{RabbitMqTopologyBuilder})" /> instead when a single
    /// shared connection is appropriate (low-traffic services, tests).
    /// </summary>
    public static BmfBuilder AddRabbitMqInboundTopology(
        this BmfBuilder builder,
        Action<IRabbitMqInboundTopologyBuilder> configure
    )
    {
        return builder.AddRabbitMqInboundTopology(RabbitMqTopology.DefaultInboundName, configure);
    }

    /// <inheritdoc cref="AddRabbitMqInboundTopology(BmfBuilder, Action{IRabbitMqInboundTopologyBuilder})" />
    public static BmfBuilder AddRabbitMqInboundTopology(
        this BmfBuilder builder,
        string topologyName,
        Action<IRabbitMqInboundTopologyBuilder> configure
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

        var topologyBuilder = new RabbitMqTopologyBuilder();
        configure(topologyBuilder);
        return AddRabbitMqTopologyCore(builder, topologyName, topologyBuilder.Build());
    }

    private static BmfBuilder AddRabbitMqTopologyCore(
        BmfBuilder builder,
        string topologyName,
        RabbitMqTopologyConfiguration configuration
    )
    {
        var services = builder.Services;
        builder.Topologies.Add(topologyName);

        foreach (var handler in configuration.Consumers.SelectMany(static consumer => consumer.Handlers))
        {
            services.TryAddScoped(handler.HandlerType);
        }

        services.AddKeyedSingleton<RabbitMqTopology>(
            topologyName,
            (serviceProvider, _) =>
            {
                var loggerFactory = serviceProvider.GetService<ILoggerFactory>() ?? NullLoggerFactory.Instance;
                RabbitMqConnectionProvider connectionProvider = new (
                    cancellationToken => CreateConnectionAsync(
                        configuration,
                        serviceProvider,
                        cancellationToken
                    ),
                    loggerFactory.CreateLogger<RabbitMqConnectionProvider>()
                );
                RabbitMqTopologyCompiler compiler = new (
                    serviceProvider.GetRequiredService<IMessageContractRegistry>(),
                    loggerFactory,
                    serializerType => (IMessageSerializer?) serviceProvider.GetService(serializerType),
                    serviceType => IsServiceRegistered(serviceProvider, serviceType)
                );
                return compiler.Compile(topologyName, configuration, connectionProvider);
            }
        );
        services.AddKeyedSingleton<Topology>(
            topologyName,
            (serviceProvider, key) => serviceProvider.GetRequiredKeyedService<RabbitMqTopology>(key)
        );
        if (string.Equals(topologyName, Topology.DefaultName, StringComparison.Ordinal))
        {
            services.TryAddSingleton<RabbitMqTopology>(
                serviceProvider => serviceProvider.GetRequiredKeyedService<RabbitMqTopology>(topologyName)
            );
        }

        services.AddSingleton<ITopologyProvisioner>(
            serviceProvider => new RabbitMqTopologyProvisioner(
                serviceProvider.GetRequiredKeyedService<RabbitMqTopology>(topologyName)
            )
        );

        if (configuration.HasInboundEndpoints)
        {
            services.AddSingleton<ITopologyRuntime>(
                serviceProvider => new RabbitMqTopologyRuntime(
                    serviceProvider.GetRequiredKeyedService<RabbitMqTopology>(topologyName),
                    serviceProvider.GetRequiredService<IServiceScopeFactory>(),
                    serviceProvider.GetService<ILogger<RabbitMqTopologyRuntime>>()
                )
            );
        }

        return builder;
    }

    private static Task<IConnection> CreateConnectionAsync(
        RabbitMqTopologyConfiguration configuration,
        IServiceProvider serviceProvider,
        CancellationToken cancellationToken
    )
    {
        var connectionFactory = configuration.CreateConnectionFactory?.Invoke(serviceProvider) ??
                                throw new TopologyValidationException(
                                    ["A RabbitMQ connection factory must be configured."]
                                );

        if (!connectionFactory.AutomaticRecoveryEnabled)
        {
            throw new TopologyValidationException(
                [
                    "RabbitMQ automatic connection recovery must be enabled. Configure ConnectionFactory.AutomaticRecoveryEnabled to true."
                ]
            );
        }

        if (configuration.HasInboundEndpoints && !connectionFactory.TopologyRecoveryEnabled)
        {
            throw new TopologyValidationException(
                [
                    "RabbitMQ topology recovery must be enabled for inbound topologies so RabbitMQ.Client can recover consumer subscriptions. Configure ConnectionFactory.TopologyRecoveryEnabled to true."
                ]
            );
        }

        return connectionFactory.CreateConnectionAsync(cancellationToken);
    }

    private static bool IsServiceRegistered(IServiceProvider serviceProvider, Type serviceType)
    {
        var serviceProviderIsService = serviceProvider.GetService<IServiceProviderIsService>();

        if (serviceProviderIsService is not null)
        {
            return serviceProviderIsService.IsService(serviceType);
        }

        return serviceProvider.GetService(serviceType) is not null;
    }
}
