using System.Threading;
using System.Threading.Tasks;

namespace Usf.Core.Messaging;

public interface IOutboundTopologyProvisioner
{
    Task ProvisionAsync(CancellationToken cancellationToken = default);
}
