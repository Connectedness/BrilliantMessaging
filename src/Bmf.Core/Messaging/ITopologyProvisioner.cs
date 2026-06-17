using System.Threading;
using System.Threading.Tasks;

namespace Bmf.Core.Messaging;

/// <summary>
/// Provisions the broker-side resources a topology requires (for example declaring exchanges, queues, and
/// bindings) before any runtime starts consuming. Transports register an implementation that the
/// <see cref="TopologyProvisioningHostedService" /> invokes during host startup.
/// </summary>
public interface ITopologyProvisioner
{
    /// <summary>
    /// Provisions the topology's broker resources.
    /// </summary>
    /// <param name="cancellationToken">A token to observe while provisioning.</param>
    /// <returns>A task that completes once provisioning has finished.</returns>
    Task ProvisionAsync(CancellationToken cancellationToken = default);
}
