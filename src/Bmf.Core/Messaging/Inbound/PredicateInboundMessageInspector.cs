using System;
using System.Threading;
using System.Threading.Tasks;

namespace Bmf.Core.Messaging.Inbound;

/// <summary>
/// An inbound message inspector that returns a pre-resolved discriminator and message type when a predicate matches
/// the transport message.
/// </summary>
public sealed class PredicateInboundMessageInspector : IInboundMessageInspector
{
    private readonly string _discriminator;
    private readonly Type _messageType;
    private readonly Func<TransportMessage, bool> _predicate;

    /// <summary>
    /// Initializes a new instance of the <see cref="PredicateInboundMessageInspector" /> class.
    /// </summary>
    /// <param name="predicate">The predicate that decides whether the inspector recognizes the delivery.</param>
    /// <param name="discriminator">The discriminator returned when the predicate matches.</param>
    /// <param name="messageType">The message type returned when the predicate matches.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="predicate" /> or <paramref name="messageType" /> is <see langword="null" />.</exception>
    /// <exception cref="ArgumentException">Thrown when <paramref name="discriminator" /> is null or whitespace.</exception>
    public PredicateInboundMessageInspector(
        Func<TransportMessage, bool> predicate,
        string discriminator,
        Type messageType
    )
    {
        _predicate = predicate ?? throw new ArgumentNullException(nameof(predicate));
        _discriminator = RequireText(discriminator, nameof(discriminator));
        _messageType = messageType ?? throw new ArgumentNullException(nameof(messageType));
    }

    /// <inheritdoc />
    public ValueTask<InboundMessageInspectionResult?> InspectAsync(
        TransportMessage transportMessage,
        CancellationToken cancellationToken = default
    )
    {
        if (transportMessage is null)
        {
            throw new ArgumentNullException(nameof(transportMessage));
        }

        return new ValueTask<InboundMessageInspectionResult?>(
            _predicate(transportMessage) ?
                new InboundMessageInspectionResult(_discriminator, _messageType) :
                null
        );
    }

    private static string RequireText(string value, string parameterName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException("The value cannot be null or whitespace.", parameterName);
        }

        return value;
    }
}
