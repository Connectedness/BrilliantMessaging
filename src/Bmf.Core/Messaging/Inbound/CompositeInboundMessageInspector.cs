using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Bmf.Core.Messaging.Inbound;

/// <summary>
/// Runs an ordered set of inbound message inspectors and returns the first non-<see langword="null" /> inspection
/// result.
/// </summary>
/// <remarks>
/// The order is significant: broad inspectors placed before narrower inspectors can shadow them.
/// </remarks>
public sealed class CompositeInboundMessageInspector : IInboundMessageInspector
{
    private readonly IReadOnlyList<IInboundMessageInspector> _inspectors;

    /// <summary>
    /// Initializes a new instance of the <see cref="CompositeInboundMessageInspector" /> class.
    /// </summary>
    /// <param name="inspectors">The inspectors to evaluate in order.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="inspectors" /> or any entry is <see langword="null" />.</exception>
    public CompositeInboundMessageInspector(IReadOnlyList<IInboundMessageInspector> inspectors)
    {
        _inspectors = inspectors ?? throw new ArgumentNullException(nameof(inspectors));

        for (var i = 0; i < inspectors.Count; i++)
        {
            if (inspectors[i] is null)
            {
                throw new ArgumentNullException(nameof(inspectors), "Inspector entries cannot be null.");
            }
        }
    }

    /// <inheritdoc />
    public async ValueTask<InboundMessageInspectionResult?> InspectAsync(
        TransportMessage transportMessage,
        CancellationToken cancellationToken = default
    )
    {
        if (transportMessage is null)
        {
            throw new ArgumentNullException(nameof(transportMessage));
        }

        foreach (var inspector in _inspectors)
        {
            var result = await inspector.InspectAsync(transportMessage, cancellationToken).ConfigureAwait(false);

            if (result is not null)
            {
                return result;
            }
        }

        return null;
    }
}
