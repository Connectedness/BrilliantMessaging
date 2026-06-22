using System;

namespace Bmf.Core.Messaging.Inbound;

/// <summary>
/// Completes a predicate-based inbound message recognizer by mapping matches to a message type.
/// </summary>
public sealed class InboundMessageRecognizerBuilder
{
    private readonly InboundMessageInspectorChainBuilder _chainBuilder;
    private readonly Func<TransportMessage, bool> _predicate;

    /// <summary>
    /// Initializes a new instance of the <see cref="InboundMessageRecognizerBuilder" /> class.
    /// </summary>
    /// <param name="chainBuilder">The parent chain builder that receives the completed recognizer entry.</param>
    /// <param name="predicate">The predicate that decides whether the recognizer matches.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="chainBuilder" /> or <paramref name="predicate" /> is <see langword="null" />.</exception>
    public InboundMessageRecognizerBuilder(
        InboundMessageInspectorChainBuilder chainBuilder,
        Func<TransportMessage, bool> predicate
    )
    {
        _chainBuilder = chainBuilder ?? throw new ArgumentNullException(nameof(chainBuilder));
        _predicate = predicate ?? throw new ArgumentNullException(nameof(predicate));
    }

    /// <summary>
    /// Maps matching deliveries to <typeparamref name="TMessage" /> and resolves the discriminator from the message
    /// contract registry at topology compilation.
    /// </summary>
    /// <typeparam name="TMessage">The message type returned when the recognizer matches.</typeparam>
    /// <returns>The parent chain builder for chaining.</returns>
    public InboundMessageInspectorChainBuilder As<TMessage>()
    {
        return _chainBuilder.AddRecognizer(_predicate, typeof(TMessage), explicitDiscriminator: null);
    }

    /// <summary>
    /// Maps matching deliveries to <typeparamref name="TMessage" /> with an explicit discriminator that does not
    /// need to be registered as a message contract.
    /// </summary>
    /// <typeparam name="TMessage">The message type returned when the recognizer matches.</typeparam>
    /// <param name="explicitDiscriminator">The discriminator returned when the recognizer matches.</param>
    /// <returns>The parent chain builder for chaining.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="explicitDiscriminator" /> is null or whitespace.</exception>
    public InboundMessageInspectorChainBuilder As<TMessage>(string explicitDiscriminator)
    {
        return string.IsNullOrWhiteSpace(explicitDiscriminator) ?
            throw new ArgumentException("The value cannot be null or whitespace.", nameof(explicitDiscriminator)) :
            _chainBuilder.AddRecognizer(_predicate, typeof(TMessage), explicitDiscriminator);
    }
}
