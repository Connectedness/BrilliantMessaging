using System.Threading;
using System.Threading.Tasks;

namespace Bmf.Core.Messaging.Inbound;

/// <summary>
/// Inspects a raw <see cref="TransportMessage" /> before pipeline dispatch to determine its discriminator and the
/// concrete message type to deserialize it into. Returning <see langword="null" /> means the inspector does not
/// recognize the delivery, allowing several inspectors to be composed in first-match-wins order.
/// </summary>
public interface IInboundMessageInspector
{
    /// <summary>
    /// Inspects the transport message and resolves its discriminator and target message type.
    /// </summary>
    /// <param name="transportMessage">The raw transport message to inspect.</param>
    /// <param name="cancellationToken">A token to observe while inspecting.</param>
    /// <returns>
    /// The inspection result describing the resolved type and discriminator, or <see langword="null" /> when the
    /// inspector does not recognize the delivery.
    /// </returns>
    ValueTask<InboundMessageInspectionResult?> InspectAsync(
        TransportMessage transportMessage,
        CancellationToken cancellationToken = default
    );
}
