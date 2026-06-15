using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;

namespace Usf.Core.Messaging;

/// <summary>
/// Discovers the registered <see cref="ITopologyRuntime" /> instances and drives their host-lifetime start/stop
/// behavior. It is registered after <see cref="TopologyProvisioningHostedService" /> so that broker resources are
/// provisioned before any topology runtime starts. Runtimes are stopped in reverse start order during host
/// shutdown so each transport can perform its own graceful drain.
/// </summary>
public sealed class TopologyRuntimeHostedService : IHostedService
{
    private readonly IReadOnlyList<ITopologyRuntime> _runtimes;

    public TopologyRuntimeHostedService(IEnumerable<ITopologyRuntime> runtimes)
    {
        if (runtimes is null)
        {
            throw new ArgumentNullException(nameof(runtimes));
        }

        _runtimes = runtimes.ToArray();
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        foreach (var runtime in _runtimes)
        {
            await runtime.StartAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        // Stop runtimes sequentially in reverse start order so each transport can drain in dependency order. A
        // single failure must not abort the loop: we make a best-effort attempt to stop every runtime and collect
        // all failures so the host can report them. Logging is intentionally omitted because the host already
        // reports propagated shutdown failures.
        List<Exception>? failures = null;
        for (var index = _runtimes.Count - 1; index >= 0; index--)
        {
            try
            {
                await _runtimes[index].StopAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                (failures ??= []).Add(exception);
            }
        }

        if (failures is null)
        {
            return;
        }

        if (failures.Count == 1)
        {
            // Rethrow the single failure unchanged so callers observe the original exception type and stack trace
            // rather than an unnecessary AggregateException wrapper.
            ExceptionDispatchInfo.Capture(failures[0]).Throw();
        }

        throw new AggregateException("One or more topology runtimes failed to stop.", failures);
    }
}
