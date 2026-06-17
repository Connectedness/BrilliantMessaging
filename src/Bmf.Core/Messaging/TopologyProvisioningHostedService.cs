using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;

namespace Bmf.Core.Messaging;

/// <summary>
/// The hosted service that runs every registered <see cref="ITopologyProvisioner" /> during host startup, before
/// the <see cref="Inbound.TopologyRuntimeHostedService" /> starts any consumers.
/// </summary>
public sealed class TopologyProvisioningHostedService : IHostedService
{
    private readonly IEnumerable<ITopologyProvisioner> _topologyProvisioners;

    /// <summary>
    /// Initializes a new instance of the <see cref="TopologyProvisioningHostedService" /> class.
    /// </summary>
    /// <param name="topologyProvisioners">The registered provisioners to run at startup.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="topologyProvisioners" /> is <see langword="null" />.</exception>
    public TopologyProvisioningHostedService(IEnumerable<ITopologyProvisioner> topologyProvisioners)
    {
        _topologyProvisioners = topologyProvisioners ??
                                throw new ArgumentNullException(nameof(topologyProvisioners));
    }

    /// <summary>
    /// Provisions every registered topology in registration order.
    /// </summary>
    /// <param name="cancellationToken">A token to observe while provisioning.</param>
    /// <returns>A task that completes when all topologies have been provisioned.</returns>
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        foreach (var topologyProvisioner in _topologyProvisioners)
        {
            await topologyProvisioner.ProvisionAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Does nothing; provisioning has no shutdown work.
    /// </summary>
    /// <param name="cancellationToken">A token to observe while stopping.</param>
    /// <returns>A completed task.</returns>
    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}
