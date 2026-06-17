using System.Threading;
using System.Threading.Tasks;

namespace Bmf.Core.Messaging.Inbound;

/// <summary>
/// Inspects a raw <see cref="TransportMessage" /> before pipeline dispatch to determine its CloudEvents
/// discriminator and the concrete message type to deserialize it into. This is the hook that resolves a wire
/// message to a known contract; the default implementation reads the CloudEvents <c>type</c> attribute.
/// </summary>
public interface IInboundMessageInspector
{
    /// <summary>
    /// Inspects the transport message and resolves its discriminator and target message type.
    /// </summary>
    /// <param name="transportMessage">The raw transport message to inspect.</param>
    /// <param name="cancellationToken">A token to observe while inspecting.</param>
    /// <returns>The inspection result describing the resolved type and discriminator.</returns>
    ValueTask<InboundMessageInspectionResult> InspectAsync(
        TransportMessage transportMessage,
        CancellationToken cancellationToken = default
    );
}
