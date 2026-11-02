using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using BrilliantMessaging.GuardClauses;

namespace BrilliantMessaging.Core.Messaging.Inbound;

/// <summary>
/// Validates declared inbound message inspector chains and compiles them into their runtime form. Transport
/// topology compilers call these members with the kind and name of the consumer they compile, which only appear in
/// diagnostic messages.
/// </summary>
public static class InboundMessageInspectorChainCompiler
{
    /// <summary>
    /// Collects the validation errors of a declared inspector chain.
    /// </summary>
    /// <param name="chain">The declared chain.</param>
    /// <param name="consumerKind">The transport's word for a consumer in diagnostic messages, for example <c>queue</c>.</param>
    /// <param name="consumerName">The name of the consumer that declares the chain.</param>
    /// <param name="isServiceRegistered">A predicate that reports whether an inspector type is registered.</param>
    /// <param name="validationErrors">The collection that receives validation errors.</param>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="consumerKind" />, <paramref name="consumerName" />,
    /// <paramref name="isServiceRegistered" />, or <paramref name="validationErrors" /> is <see langword="null" />.
    /// </exception>
    /// <exception cref="BrilliantMessaging.GuardClauses.Exceptions.EmptyStringException">
    /// Thrown when <paramref name="consumerKind" /> or <paramref name="consumerName" /> is empty.
    /// </exception>
    /// <exception cref="BrilliantMessaging.GuardClauses.Exceptions.WhiteSpaceStringException">
    /// Thrown when <paramref name="consumerKind" /> or <paramref name="consumerName" /> contains only whitespace.
    /// </exception>
    public static void Validate(
        ImmutableArray<InboundMessageInspectorChainEntry> chain,
        string consumerKind,
        string consumerName,
        Func<Type, bool> isServiceRegistered,
        ICollection<string> validationErrors
    )
    {
        var consumer = DescribeConsumer(consumerKind, consumerName);
        isServiceRegistered.MustNotBeNull();
        validationErrors.MustNotBeNull();

        if (chain.Length == 0)
        {
            validationErrors.Add($"Inbound inspector chain for {consumer} must contain at least one entry.");
            return;
        }

        foreach (var entry in chain)
        {
            switch (entry)
            {
                case ServiceInboundMessageInspectorChainEntry serviceEntry:
                    if (!typeof(IInboundMessageInspector).IsAssignableFrom(serviceEntry.InspectorType))
                    {
                        validationErrors.Add(
                            $"Inbound inspector '{serviceEntry.InspectorType}' for {consumer} does not implement '{typeof(IInboundMessageInspector)}'."
                        );
                    }
                    else if (!isServiceRegistered(serviceEntry.InspectorType))
                    {
                        validationErrors.Add(
                            $"Inbound inspector '{serviceEntry.InspectorType}' for {consumer} is not registered."
                        );
                    }

                    break;
                case RecognizerInboundMessageInspectorChainEntry:
                    break;
                case null:
                    validationErrors.Add($"Inbound inspector chain for {consumer} contains a null entry.");
                    break;
                default:
                    validationErrors.Add(
                        $"Inbound inspector chain entry '{GetTypeName(entry.GetType())}' for {consumer} is not supported."
                    );
                    break;
            }
        }
    }

    /// <summary>
    /// Resolves the discriminator of every recognizer in a declared inspector chain.
    /// </summary>
    /// <param name="chain">The declared chain.</param>
    /// <param name="effectiveMessageContracts">The topology's effective message contract registry.</param>
    /// <param name="consumerKind">The transport's word for a consumer in diagnostic messages, for example <c>queue</c>.</param>
    /// <param name="consumerName">The name of the consumer that declares the chain.</param>
    /// <param name="validationErrors">
    /// The collection that receives an error for each recognizer whose message type is not registered, or
    /// <see langword="null" /> to let <see cref="MessageContractNotRegisteredException" /> propagate after validation
    /// already passed.
    /// </param>
    /// <returns>The resolved recognizers in declaration order.</returns>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="effectiveMessageContracts" />, <paramref name="consumerKind" />, or
    /// <paramref name="consumerName" /> is <see langword="null" />.
    /// </exception>
    /// <exception cref="BrilliantMessaging.GuardClauses.Exceptions.EmptyStringException">
    /// Thrown when <paramref name="consumerKind" /> or <paramref name="consumerName" /> is empty.
    /// </exception>
    /// <exception cref="BrilliantMessaging.GuardClauses.Exceptions.WhiteSpaceStringException">
    /// Thrown when <paramref name="consumerKind" /> or <paramref name="consumerName" /> contains only whitespace.
    /// </exception>
    public static IReadOnlyList<ResolvedInboundMessageRecognizer> ResolveRecognizers(
        ImmutableArray<InboundMessageInspectorChainEntry> chain,
        IMessageContractRegistry effectiveMessageContracts,
        string consumerKind,
        string consumerName,
        ICollection<string>? validationErrors
    )
    {
        effectiveMessageContracts.MustNotBeNull();
        var consumer = DescribeConsumer(consumerKind, consumerName);

        List<ResolvedInboundMessageRecognizer> recognizers = [];

        foreach (var recognizerEntry in chain.OfType<RecognizerInboundMessageInspectorChainEntry>())
        {
            string discriminator;

            try
            {
                discriminator = ResolveDiscriminator(recognizerEntry, effectiveMessageContracts);
            }
            catch (MessageContractNotRegisteredException) when (validationErrors is not null)
            {
                validationErrors.Add(
                    $"Inbound recognizer for message '{GetTypeName(recognizerEntry.MessageType)}' on {consumer} uses As<T>() but the type is not registered. Use As<T>(explicitDiscriminator) or register the contract with MessageContractRegistryBuilder.Map<T>(...)."
                );
                continue;
            }

            recognizers.Add(new ResolvedInboundMessageRecognizer(recognizerEntry, discriminator));
        }

        return recognizers;
    }

    /// <summary>
    /// Maps resolved recognizers to the consumer's handlers: the handler for exactly the recognizer's message type
    /// when there is one, otherwise the only handler whose message type is assignable from it.
    /// </summary>
    /// <typeparam name="THandler">The transport's handler definition type.</typeparam>
    /// <param name="recognizers">The resolved recognizers.</param>
    /// <param name="handlers">The consumer's handlers.</param>
    /// <param name="getMessageType">A function that returns the message type of a handler.</param>
    /// <param name="consumerKind">The transport's word for a consumer in diagnostic messages, for example <c>queue</c>.</param>
    /// <param name="consumerName">The name of the consumer that declares the chain.</param>
    /// <param name="validationErrors">
    /// The collection that receives an error for each recognizer without exactly one matching handler, or
    /// <see langword="null" /> to skip those recognizers silently after validation already passed.
    /// </param>
    /// <returns>The recognizers that map to exactly one handler.</returns>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="recognizers" />, <paramref name="handlers" />, <paramref name="getMessageType" />,
    /// <paramref name="consumerKind" />, or <paramref name="consumerName" /> is <see langword="null" />.
    /// </exception>
    /// <exception cref="BrilliantMessaging.GuardClauses.Exceptions.EmptyStringException">
    /// Thrown when <paramref name="consumerKind" /> or <paramref name="consumerName" /> is empty.
    /// </exception>
    /// <exception cref="BrilliantMessaging.GuardClauses.Exceptions.WhiteSpaceStringException">
    /// Thrown when <paramref name="consumerKind" /> or <paramref name="consumerName" /> contains only whitespace.
    /// </exception>
    public static IReadOnlyList<InboundRecognizerHandlerMapping<THandler>> MapRecognizersToHandlers<THandler>(
        IReadOnlyList<ResolvedInboundMessageRecognizer> recognizers,
        IReadOnlyList<THandler> handlers,
        Func<THandler, Type> getMessageType,
        string consumerKind,
        string consumerName,
        ICollection<string>? validationErrors
    )
        where THandler : class
    {
        recognizers.MustNotBeNull();
        handlers.MustNotBeNull();
        getMessageType.MustNotBeNull();
        var consumer = DescribeConsumer(consumerKind, consumerName);

        List<InboundRecognizerHandlerMapping<THandler>> mappings = [];

        foreach (var recognizer in recognizers)
        {
            var matchingHandlers = handlers
               .Where(handler => getMessageType(handler) == recognizer.Entry.MessageType)
               .ToArray();

            if (matchingHandlers.Length == 0)
            {
                matchingHandlers = handlers
                   .Where(handler => getMessageType(handler).IsAssignableFrom(recognizer.Entry.MessageType))
                   .ToArray();
            }

            if (matchingHandlers.Length == 0)
            {
                validationErrors?.Add(
                    $"Inbound recognizer for message '{GetTypeName(recognizer.Entry.MessageType)}' maps discriminator '{recognizer.Discriminator}' on {consumer}, but no handler on that {consumerKind} handles an assignable message type."
                );
                continue;
            }

            if (matchingHandlers.Length > 1)
            {
                validationErrors?.Add(
                    $"Inbound recognizer for message '{GetTypeName(recognizer.Entry.MessageType)}' maps discriminator '{recognizer.Discriminator}' on {consumer}, but multiple handlers on that {consumerKind} handle an assignable message type."
                );
                continue;
            }

            mappings.Add(new InboundRecognizerHandlerMapping<THandler>(recognizer, matchingHandlers[0]));
        }

        return mappings;
    }

    /// <summary>
    /// Compiles a validated inspector chain. Recognizers become shared instances, and entries for
    /// <see cref="CloudEventsInboundMessageInspector" /> become an instance bound to
    /// <paramref name="effectiveMessageContracts" />, because the dependency-injection singleton only knows the
    /// canonical contracts and would miss topology-local ones.
    /// </summary>
    /// <param name="chain">The declared chain.</param>
    /// <param name="effectiveMessageContracts">The topology's effective message contract registry.</param>
    /// <param name="consumerKind">The transport's word for a consumer in diagnostic messages, for example <c>queue</c>.</param>
    /// <param name="consumerName">The name of the consumer that declares the chain.</param>
    /// <returns>The compiled chain.</returns>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="effectiveMessageContracts" />, <paramref name="consumerKind" />, or
    /// <paramref name="consumerName" /> is <see langword="null" />.
    /// </exception>
    /// <exception cref="BrilliantMessaging.GuardClauses.Exceptions.EmptyStringException">
    /// Thrown when <paramref name="consumerKind" /> or <paramref name="consumerName" /> is empty.
    /// </exception>
    /// <exception cref="BrilliantMessaging.GuardClauses.Exceptions.WhiteSpaceStringException">
    /// Thrown when <paramref name="consumerKind" /> or <paramref name="consumerName" /> contains only whitespace.
    /// </exception>
    /// <exception cref="InvalidOperationException">Thrown when the chain contains a null or unsupported entry.</exception>
    public static CompiledInboundMessageInspectorChain Compile(
        ImmutableArray<InboundMessageInspectorChainEntry> chain,
        IMessageContractRegistry effectiveMessageContracts,
        string consumerKind,
        string consumerName
    )
    {
        effectiveMessageContracts.MustNotBeNull();
        var consumer = DescribeConsumer(consumerKind, consumerName);

        List<CompiledInboundMessageInspectorChainEntry> entries = [];

        foreach (var entry in chain)
        {
            switch (entry)
            {
                case ServiceInboundMessageInspectorChainEntry serviceEntry
                    when serviceEntry.InspectorType == typeof(CloudEventsInboundMessageInspector):
                    entries.Add(
                        new CompiledInstanceInboundMessageInspectorChainEntry(
                            new CloudEventsInboundMessageInspector(effectiveMessageContracts)
                        )
                    );
                    break;
                case ServiceInboundMessageInspectorChainEntry serviceEntry:
                    entries.Add(new CompiledServiceInboundMessageInspectorChainEntry(serviceEntry.InspectorType));
                    break;
                case RecognizerInboundMessageInspectorChainEntry recognizerEntry:
                    entries.Add(
                        new CompiledInstanceInboundMessageInspectorChainEntry(
                            new PredicateInboundMessageInspector(
                                recognizerEntry.Predicate,
                                ResolveDiscriminator(recognizerEntry, effectiveMessageContracts),
                                recognizerEntry.MessageType
                            )
                        )
                    );
                    break;
                case null:
                    throw new InvalidOperationException($"Inbound inspector chain for {consumer} contains a null entry.");
                default:
                    throw new InvalidOperationException(
                        $"Inbound inspector chain entry '{GetTypeName(entry.GetType())}' for {consumer} is not supported."
                    );
            }
        }

        return new CompiledInboundMessageInspectorChain(entries.AsReadOnly());
    }

    private static string ResolveDiscriminator(
        RecognizerInboundMessageInspectorChainEntry recognizerEntry,
        IMessageContractRegistry effectiveMessageContracts
    )
    {
        return recognizerEntry.ExplicitDiscriminator ??
               effectiveMessageContracts.GetDiscriminator(recognizerEntry.MessageType);
    }

    private static string DescribeConsumer(string consumerKind, string consumerName)
    {
        return $"{consumerKind.MustNotBeNullOrWhiteSpace()} '{consumerName.MustNotBeNullOrWhiteSpace()}'";
    }

    private static string GetTypeName(Type type)
    {
        return type.FullName ?? type.Name;
    }
}

/// <summary>
/// A recognizer chain entry together with the discriminator it resolves to on its topology.
/// </summary>
public sealed class ResolvedInboundMessageRecognizer
{
    /// <summary>
    /// Initializes a new instance of the <see cref="ResolvedInboundMessageRecognizer" /> class.
    /// </summary>
    /// <param name="entry">The declared recognizer entry.</param>
    /// <param name="discriminator">The resolved discriminator.</param>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="entry" /> or <paramref name="discriminator" /> is <see langword="null" />.
    /// </exception>
    /// <exception cref="BrilliantMessaging.GuardClauses.Exceptions.EmptyStringException">
    /// Thrown when <paramref name="discriminator" /> is empty.
    /// </exception>
    /// <exception cref="BrilliantMessaging.GuardClauses.Exceptions.WhiteSpaceStringException">
    /// Thrown when <paramref name="discriminator" /> contains only whitespace.
    /// </exception>
    public ResolvedInboundMessageRecognizer(RecognizerInboundMessageInspectorChainEntry entry, string discriminator)
    {
        Entry = entry.MustNotBeNull();
        Discriminator = discriminator.MustNotBeNullOrWhiteSpace();
    }

    /// <summary>
    /// Gets the declared recognizer entry.
    /// </summary>
    public RecognizerInboundMessageInspectorChainEntry Entry { get; }

    /// <summary>
    /// Gets the resolved discriminator.
    /// </summary>
    public string Discriminator { get; }
}

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
