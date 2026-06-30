using System;
using BrilliantMessaging.Transport.InMemory.Inbound;

namespace BrilliantMessaging.Transport.InMemory;

/// <summary>
/// Configures the inbound side of an in-memory topology: declaring topics and consuming from them.
/// </summary>
public interface IInMemoryInboundTopologyBuilder
{
    /// <summary>
    /// Declares a topic resource. Topics referenced by <c>ToTopic</c> and <c>Consume</c> must be declared.
    /// </summary>
    /// <param name="topic">The topic name to declare.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="topic" /> is null or whitespace.</exception>
    /// <exception cref="InvalidOperationException">Thrown when <paramref name="topic" /> is already declared.</exception>
    IInMemoryInboundTopologyBuilder Topic(string topic);

    /// <summary>
    /// Consumes messages from a declared topic. Each <c>Consume</c> call adds an independent consumer route;
    /// multiple routes on the same topic each receive a fanout copy of every published message.
    /// </summary>
    /// <param name="topic">The declared topic to consume from.</param>
    /// <param name="configure">A callback that configures the consumer.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="topic" /> is null or whitespace.</exception>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="configure" /> is <see langword="null" />.</exception>
    IInMemoryInboundTopologyBuilder Consume(string topic, Action<InMemoryInboundConsumerBuilder> configure);

    /// <summary>
    /// Sets the graceful shutdown timeout the runtime uses to drain in-flight deliveries before cancelling them.
    /// </summary>
    /// <param name="timeout">The shutdown timeout; must be positive or <see cref="System.Threading.Timeout.InfiniteTimeSpan" />.</param>
    /// <returns>The same builder for chaining.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="timeout" /> is not positive and not infinite.</exception>
    IInMemoryInboundTopologyBuilder ShutdownTimeout(TimeSpan timeout);
}
