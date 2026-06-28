using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using BrilliantMessaging.Core.Messaging;

namespace BrilliantMessaging.Transport.RabbitMq;

/// <summary>
/// Fluent builder for a binding from a source exchange to a queue, collecting the routing key, binding mode, and
/// binding arguments into a <see cref="RabbitMqQueueBindingDefinition" />.
/// </summary>
public sealed class RabbitMqQueueBindingBuilder : IBuildable<RabbitMqQueueBindingDefinition>
{
    private readonly Dictionary<string, object?> _arguments = new (StringComparer.Ordinal);

    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMqQueueBindingBuilder" /> class.
    /// </summary>
    /// <param name="exchangeName">The name of the source exchange.</param>
    /// <param name="queueName">The name of the bound queue.</param>
    /// <param name="routingKey">The binding routing key; an empty string is permitted.</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="exchangeName" /> or <paramref name="queueName" /> is null or whitespace.</exception>
    public RabbitMqQueueBindingBuilder(string exchangeName, string queueName, string routingKey)
    {
        SourceExchangeName = RequireText(exchangeName, nameof(exchangeName));
        QueueName = RequireText(queueName, nameof(queueName));
        RoutingKey = routingKey ?? string.Empty;
        BindingMode = RabbitMqBindingMode.Active;
    }

    /// <summary>
    /// Gets the name of the source exchange.
    /// </summary>
    public string SourceExchangeName { get; }

    /// <summary>
    /// Gets the name of the bound queue.
    /// </summary>
    public string QueueName { get; }

    /// <summary>
    /// Gets the binding routing key.
    /// </summary>
    public string RoutingKey { get; }

    /// <summary>
    /// Gets the binding mode that controls whether and how the binding is declared at provisioning time.
    /// </summary>
    public RabbitMqBindingMode BindingMode { get; private set; }

    /// <inheritdoc />
    RabbitMqQueueBindingDefinition IBuildable<RabbitMqQueueBindingDefinition>.Build()
    {
        return new RabbitMqQueueBindingDefinition(
            SourceExchangeName,
            QueueName,
            RoutingKey,
            BindingMode,
            new ReadOnlyDictionary<string, object?>(_arguments)
        );
    }

    /// <summary>
    /// Sets the binding mode for the binding.
    /// </summary>
    /// <param name="bindingMode">The binding mode to use.</param>
    /// <returns>The same builder for chaining.</returns>
    public RabbitMqQueueBindingBuilder WithBindingMode(RabbitMqBindingMode bindingMode)
    {
        BindingMode = bindingMode;
        return this;
    }

    /// <summary>
    /// Adds a binding argument (for example a header match for a headers exchange).
    /// </summary>
    /// <param name="name">The argument name.</param>
    /// <param name="value">The argument value.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="name" /> is null or whitespace.</exception>
    public RabbitMqQueueBindingBuilder WithArgument(string name, object? value)
    {
        _arguments[RequireText(name, nameof(name))] = value;
        return this;
    }

    /// <summary>
    /// Sets the <c>x-match</c> mode for a headers-exchange binding, controlling whether the broker requires all
    /// configured headers to match or any one of them, and whether <c>x-</c>-prefixed headers participate in
    /// matching.
    /// </summary>
    /// <param name="match">The header-match mode.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <remarks>
    /// This method always writes the <c>x-match</c> argument, overriding any value set by a previous
    /// <see cref="WithHeaderMatch" /> or <see cref="WithArgument" /> call.
    /// </remarks>
    public RabbitMqQueueBindingBuilder WithHeaderMatch(RabbitMqHeaderMatch match)
    {
        _arguments["x-match"] = MapHeaderMatch(match);
        return this;
    }

    /// <summary>
    /// Adds a header match predicate for a headers-exchange binding. The header name and value are recorded as
    /// a binding argument the broker matches against each published message's headers.
    /// </summary>
    /// <param name="name">
    /// The header name. Must not be <c>x-match</c>, which is the match-mode control argument
    /// rather than a normal header predicate; use <see cref="WithHeaderMatch" /> to configure the match mode, or
    /// <see cref="WithArgument" /> to bypass this guard.
    /// </param>
    /// <param name="value">The header value to match.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="name" /> is null or whitespace, or when
    /// <paramref name="name" /> is <c>x-match</c>.
    /// </exception>
    /// <remarks>
    /// When no <c>x-match</c> argument has been set yet (by <see cref="WithHeaderMatch" /> or a prior
    /// <see cref="WithHeader" /> call), this method writes a default <c>x-match</c> of <c>all</c> so a single
    /// <see cref="WithHeader" /> call is unambiguous rather than leaving <c>x-match</c> to the broker's
    /// version-dependent default. For <c>x-</c>-prefixed header predicates, call
    /// <see cref="WithHeaderMatch" /> with <see cref="RabbitMqHeaderMatch.AllWithX" /> or
    /// <see cref="RabbitMqHeaderMatch.AnyWithX" />; Active topology bindings that leave those predicates under
    /// plain <c>all</c> or <c>any</c> are rejected by the topology compiler because RabbitMQ ignores them.
    /// </remarks>
    public RabbitMqQueueBindingBuilder WithHeader(string name, object? value)
    {
        var validatedName = RequireText(name, nameof(name));

        if (string.Equals(validatedName, "x-match", StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "The 'x-match' header is the match-mode control argument; use WithHeaderMatch to set it.",
                nameof(name)
            );
        }

        if (!_arguments.ContainsKey("x-match"))
        {
            _arguments["x-match"] = "all";
        }

        _arguments[validatedName] = value;
        return this;
    }

    private static string MapHeaderMatch(RabbitMqHeaderMatch match)
    {
        return match switch
        {
            RabbitMqHeaderMatch.All => "all",
            RabbitMqHeaderMatch.Any => "any",
            RabbitMqHeaderMatch.AllWithX => "all-with-x",
            RabbitMqHeaderMatch.AnyWithX => "any-with-x",
            _ => throw new ArgumentOutOfRangeException(nameof(match), match, "Unsupported header match mode.")
        };
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
