using System;
using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging.Inbound;
using RabbitMQ.Client;

namespace BrilliantMessaging.Transport.RabbitMq.Inbound;

/// <summary>
/// The RabbitMQ <see cref="IMessageAcknowledgement" />. It acks or rejects a single delivery by its delivery tag,
/// settling at most once so a duplicate settlement is a no-op.
/// </summary>
public sealed class RabbitMqMessageAcknowledgement : IMessageAcknowledgement
{
    private readonly IChannel _channel;
    private readonly ulong _deliveryTag;
    private int _settled;

    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMqMessageAcknowledgement" /> class.
    /// </summary>
    /// <param name="channel">The channel the delivery arrived on.</param>
    /// <param name="deliveryTag">The delivery tag identifying the message.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="channel" /> is <see langword="null" />.</exception>
    public RabbitMqMessageAcknowledgement(IChannel channel, ulong deliveryTag)
    {
        _channel = channel ?? throw new ArgumentNullException(nameof(channel));
        _deliveryTag = deliveryTag;
    }

    /// <inheritdoc />
    public Task AckAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref _settled, 1) != 0)
        {
            return Task.CompletedTask;
        }

        return _channel.BasicAckAsync(_deliveryTag, multiple: false, cancellationToken).AsTask();
    }

    /// <inheritdoc />
    public Task NackAsync(bool requeue, CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref _settled, 1) != 0)
        {
            return Task.CompletedTask;
        }

        // We settle exactly one delivery, so basic.reject gives us the same requeue/drop choice as
        // basic.nack(multiple: false). RabbitMQ 4.3 quorum queues count basic.reject redeliveries toward
        // delivery-limit; basic.nack requeues only increment acquired-count and would not exhaust that limit.
        // See https://www.rabbitmq.com/docs/quorum-queues#when-is-delivery-count-incremented
        return _channel.BasicRejectAsync(_deliveryTag, requeue, cancellationToken).AsTask();
    }
}
