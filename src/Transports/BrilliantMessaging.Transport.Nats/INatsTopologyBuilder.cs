using System;
using BrilliantMessaging.Core.Messaging;
using NATS.Client.Core;

namespace BrilliantMessaging.Transport.Nats;

/// <summary>
/// Shared NATS topology surface. All configured messaging resources are JetStream-backed; core NATS pub/sub is
/// not implemented by this package.
/// </summary>
public interface INatsTopologyBuilder<out TSelf>
    where TSelf : INatsTopologyBuilder<TSelf>
{
    /// <summary>
    /// Configures the NATS server URI.
    /// </summary>
    TSelf UseServer(string serverUrl);

    /// <summary>
    /// Configures concrete NATS client options.
    /// </summary>
    TSelf UseOptions(NatsOpts options);

    /// <summary>
    /// Configures NATS client options from the application service provider.
    /// </summary>
    TSelf UseOptions(Func<IServiceProvider, NatsOpts> createOptions);

    /// <summary>
    /// Configures topology-local message contracts used by this NATS topology.
    /// </summary>
    TSelf MapMessageContracts(Action<MessageContractRegistryBuilder> configure);

    /// <summary>
    /// Declares a JetStream stream and its subject patterns.
    /// </summary>
    TSelf Stream(string name, Action<NatsStreamBuilder> configure);

    /// <summary>
    /// Selects whether JetStream resources are actively created/updated or asserted only.
    /// </summary>
    TSelf Provisioning(NatsTopologyProvisioningMode mode);
}
