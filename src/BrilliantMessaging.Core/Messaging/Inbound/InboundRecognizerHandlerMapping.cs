using System;
using BrilliantMessaging.GuardClauses;

namespace BrilliantMessaging.Core.Messaging.Inbound;

/// <summary>
/// Associates a resolved recognizer with the handler its discriminator dispatches to.
/// </summary>
/// <typeparam name="THandler">The transport's handler definition type.</typeparam>
public sealed class InboundRecognizerHandlerMapping<THandler>
    where THandler : class
{
    /// <summary>
    /// Initializes a new instance of the <see cref="InboundRecognizerHandlerMapping{THandler}" /> class.
    /// </summary>
    /// <param name="recognizer">The resolved recognizer.</param>
    /// <param name="handler">The handler the recognizer maps to.</param>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="recognizer" /> or <paramref name="handler" /> is <see langword="null" />.
    /// </exception>
    public InboundRecognizerHandlerMapping(ResolvedInboundMessageRecognizer recognizer, THandler handler)
    {
        Recognizer = recognizer.MustNotBeNull();
        Handler = handler.MustNotBeNull();
    }

    /// <summary>
    /// Gets the resolved recognizer.
    /// </summary>
    public ResolvedInboundMessageRecognizer Recognizer { get; }

    /// <summary>
    /// Gets the handler the recognizer maps to.
    /// </summary>
    public THandler Handler { get; }
}
