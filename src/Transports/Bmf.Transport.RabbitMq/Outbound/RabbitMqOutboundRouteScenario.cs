namespace Bmf.Transport.RabbitMq.Outbound;

/// <summary>
/// Identifies how an outbound target routes published messages to its exchange, matching the exchange type
/// selected on the <see cref="RabbitMqOutboundTargetBuilder{TMessage}" />.
/// </summary>
public enum RabbitMqOutboundRouteScenario
{
    /// <summary>
    /// Publishes to a fanout exchange, which broadcasts to all bound queues.
    /// </summary>
    Fanout = 0,

    /// <summary>
    /// Publishes to a direct exchange using an exact routing-key match.
    /// </summary>
    Direct = 1,

    /// <summary>
    /// Publishes to a topic exchange using a pattern routing-key match.
    /// </summary>
    Topic = 2,

    /// <summary>
    /// Publishes to a headers exchange, which routes on message headers rather than a routing key.
    /// </summary>
    Headers = 3
}
