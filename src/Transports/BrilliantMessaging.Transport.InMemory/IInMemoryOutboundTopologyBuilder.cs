using System;
using BrilliantMessaging.Transport.InMemory.Outbound;

namespace BrilliantMessaging.Transport.InMemory;

/// <summary>
/// Configures the outbound side of an in-memory topology: declaring topics and mapping message types to them.
/// </summary>
public interface IInMemoryOutboundTopologyBuilder
{
    /// <summary>
    /// Declares a topic resource. Topics referenced by <c>ToTopic</c> and <c>Consume</c> must be declared.
    /// </summary>
    /// <param name="topic">The topic name to declare.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="topic" /> is null or whitespace.</exception>
    /// <exception cref="InvalidOperationException">Thrown when <paramref name="topic" /> is already declared.</exception>
    IInMemoryOutboundTopologyBuilder Topic(string topic);

    /// <summary>
    /// Maps outbound messages of type <typeparamref name="TMessage" /> to a declared topic.
    /// </summary>
    /// <param name="configure">A callback that configures the outbound target.</param>
    /// <typeparam name="TMessage">The message type the target publishes.</typeparam>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="configure" /> is <see langword="null" />.</exception>
    IInMemoryOutboundTopologyBuilder Publish<TMessage>(Action<InMemoryOutboundTargetBuilder<TMessage>> configure);

    /// <summary>
    /// Sets the graceful shutdown timeout the runtime uses to drain in-flight deliveries before cancelling them.
    /// </summary>
    /// <param name="timeout">The shutdown timeout; must be positive or <see cref="System.Threading.Timeout.InfiniteTimeSpan" />.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="timeout" /> is not positive and not infinite.</exception>
    IInMemoryOutboundTopologyBuilder ShutdownTimeout(TimeSpan timeout);

    /// <summary>
    /// Records every routed message for inspection through <see cref="InMemoryBroker.GetMessages" />.
    /// </summary>
    /// <returns>The same builder for chaining.</returns>
    IInMemoryOutboundTopologyBuilder RecordMessages();

    /// <summary>
    /// Enables or disables routed-message recording. Passing <see langword="true" /> is equivalent to
    /// <see cref="RecordMessages()" />.
    /// </summary>
    /// <param name="record"><see langword="true" /> to record every routed message; <see langword="false" /> to disable recording.</param>
    /// <returns>The same builder for chaining.</returns>
    IInMemoryOutboundTopologyBuilder RecordMessages(bool record);

    /// <summary>
    /// Records at most <paramref name="maxPerTopic" /> routed messages per topic for inspection through
    /// <see cref="InMemoryBroker.GetMessages" />.
    /// </summary>
    /// <param name="maxPerTopic">The maximum number of recorded messages retained per topic. The value must be positive.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="maxPerTopic" /> is not positive.</exception>
    IInMemoryOutboundTopologyBuilder RecordMessages(int maxPerTopic);
}
