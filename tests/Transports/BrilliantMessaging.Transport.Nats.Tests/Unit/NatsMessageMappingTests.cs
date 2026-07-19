using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Transport.Nats.Inbound;
using FluentAssertions;
using NATS.Client.Core;
using NATS.Client.JetStream;
using Xunit;

namespace BrilliantMessaging.Transport.Nats.Tests.Unit;

public sealed class NatsMessageMappingTests
{
    [Fact]
    public void TransportMessage_ExposesBodyHeadersAndDeliveryAttempt()
    {
        Dictionary<string, object?> headers = new (StringComparer.Ordinal)
        {
            ["cloudEvents:type"] = "tests.order.placed",
            ["content-type"] = "application/json",
            ["message-id"] = "message-1"
        };

        NatsTransportMessage message = new (
            "orders.placed",
            "body"u8.ToArray(),
            headers,
            "application/json",
            null,
            "message-1",
            3
        );

        message.Source.Should().Be("orders.placed");
        message.Body.ToArray().Should().Equal("body"u8.ToArray());
        message.ContentType.Should().Be("application/json");
        message.MessageId.Should().Be("message-1");
        message.Redelivered.Should().BeTrue();
        message.DeliveryAttempt.Should().Be(3);
        message.TryGetHeaderString("cloudEvents:type", out var type).Should().BeTrue();
        type.Should().Be("tests.order.placed");
    }

    [Fact]
    public async Task Acknowledgement_AckUsesJetStreamAck()
    {
        FakeJetStreamMessage message = new ();
        NatsMessageAcknowledgement acknowledgement = new (
            message,
            TimeSpan.FromSeconds(2),
            1,
            5,
            _ => throw new InvalidOperationException()
        );

        await acknowledgement.AckAsync(TestContext.Current.CancellationToken);

        message.AckCount.Should().Be(1);
        message.NakCount.Should().Be(0);
        message.TermCount.Should().Be(0);
    }

    [Fact]
    public async Task Acknowledgement_RequeueUsesDelayedNak()
    {
        FakeJetStreamMessage message = new ();
        NatsMessageAcknowledgement acknowledgement = new (
            message,
            TimeSpan.FromSeconds(2),
            1,
            5,
            _ => throw new InvalidOperationException()
        );

        await acknowledgement.NackAsync(requeue: true, TestContext.Current.CancellationToken);

        message.NakCount.Should().Be(1);
        message.LastNakDelay.Should().Be(TimeSpan.FromSeconds(2));
        message.TermCount.Should().Be(0);
    }

    [Fact]
    public async Task Acknowledgement_RequeueSendsImmediateNakEvenAtFinalDeliveryAttempt()
    {
        FakeJetStreamMessage message = new ();
        NatsMessageAcknowledgement acknowledgement = new (
            message,
            TimeSpan.FromSeconds(2),
            5,
            5,
            _ => throw new InvalidOperationException()
        );

        await acknowledgement.RequeueAsync(TestContext.Current.CancellationToken);

        message.NakCount.Should().Be(1);
        message.LastNakDelay.Should().BeNull();
        message.TermCount.Should().Be(0);
    }

    [Fact]
    public async Task Acknowledgement_RequeueIsNoOpWhenAlreadySettled()
    {
        FakeJetStreamMessage message = new ();
        NatsMessageAcknowledgement acknowledgement = new (
            message,
            TimeSpan.FromSeconds(2),
            1,
            5,
            _ => throw new InvalidOperationException()
        );

        await acknowledgement.AckAsync(TestContext.Current.CancellationToken);
        await acknowledgement.RequeueAsync(TestContext.Current.CancellationToken);

        message.AckCount.Should().Be(1);
        message.NakCount.Should().Be(0);
    }

    [Fact]
    public async Task Acknowledgement_RequeueSettlesTheDelivery()
    {
        FakeJetStreamMessage message = new ();
        NatsMessageAcknowledgement acknowledgement = new (
            message,
            TimeSpan.FromSeconds(2),
            1,
            5,
            _ => throw new InvalidOperationException()
        );

        await acknowledgement.RequeueAsync(TestContext.Current.CancellationToken);
        await acknowledgement.AckAsync(TestContext.Current.CancellationToken);

        message.NakCount.Should().Be(1);
        message.AckCount.Should().Be(0);
    }

    [Fact]
    public async Task Acknowledgement_RejectPublishesDeadLetterBeforeTerminate()
    {
        FakeJetStreamMessage message = new ();
        List<string> operations = [];
        NatsMessageAcknowledgement acknowledgement = new (
            message,
            TimeSpan.FromSeconds(2),
            1,
            5,
            _ =>
            {
                operations.Add("dead-letter");
                return Task.FromResult(true);
            }
        );
        message.OnTerminate = () => operations.Add("term");

        await acknowledgement.NackAsync(requeue: false, TestContext.Current.CancellationToken);

        operations.Should().Equal("dead-letter", "term");
        message.TermCount.Should().Be(1);
        message.LastTerminateReason.Should().Be("Dead-lettered by Brilliant Messaging.");
    }

    [Fact]
    public async Task Acknowledgement_RetryExhaustionPublishesDeadLetterBeforeTerminate()
    {
        FakeJetStreamMessage message = new ();
        List<string> operations = [];
        NatsMessageAcknowledgement acknowledgement = new (
            message,
            TimeSpan.FromSeconds(2),
            5,
            5,
            _ =>
            {
                operations.Add("dead-letter");
                return Task.FromResult(true);
            }
        );
        message.OnTerminate = () => operations.Add("term");

        await acknowledgement.NackAsync(requeue: true, TestContext.Current.CancellationToken);

        message.NakCount.Should().Be(0);
        operations.Should().Equal("dead-letter", "term");
        message.TermCount.Should().Be(1);
        message.LastTerminateReason.Should().Be("Dead-lettered by Brilliant Messaging.");
    }

    [Fact]
    public async Task Acknowledgement_RejectWithoutDeadLetterUsesTerminateReason()
    {
        FakeJetStreamMessage message = new ();
        NatsMessageAcknowledgement acknowledgement = new (
            message,
            TimeSpan.FromSeconds(2),
            1,
            5,
            _ => Task.FromResult(false)
        );

        await acknowledgement.NackAsync(requeue: false, TestContext.Current.CancellationToken);

        message.TermCount.Should().Be(1);
        message.LastTerminateReason.Should().Be("Terminated by Brilliant Messaging.");
    }

    [Fact]
    public void DeadLetterMessageId_DerivesFromOriginalNatsMsgId()
    {
        FakeJetStreamMessage message = new ();
        message.Headers!["Nats-Msg-Id"] = "event-1";

        var messageId = NatsTopologyRuntime.GetDeadLetterMessageId(CreateConsumer("orders.dead"), message);

        messageId.Should().Be("event-1:dlq:orders-worker:orders.dead");
    }

    [Fact]
    public void DeadLetterMessageId_FallsBackToStreamSequenceWithoutOriginalNatsMsgId()
    {
        FakeJetStreamMessage message = new ()
        {
            Metadata = new NatsJSMsgMetadata(
                new NatsJSSequencePair(42, 7),
                1,
                0,
                DateTimeOffset.UtcNow,
                "ORDERS",
                "orders-worker",
                string.Empty
            )
        };

        var messageId = NatsTopologyRuntime.GetDeadLetterMessageId(CreateConsumer("orders.dead"), message);

        messageId.Should().Be("ORDERS:42:dlq:orders-worker:orders.dead");
    }

    [Fact]
    public void DeadLetterMessageId_IsNullWithoutDeadLetterSubject()
    {
        FakeJetStreamMessage message = new ();
        message.Headers!["Nats-Msg-Id"] = "event-1";

        var messageId = NatsTopologyRuntime.GetDeadLetterMessageId(CreateConsumer(null), message);

        messageId.Should().BeNull();
    }

    [Fact]
    public void DeadLetterMessageId_IsNullWithoutAnyStableIdSource()
    {
        FakeJetStreamMessage message = new ();

        var messageId = NatsTopologyRuntime.GetDeadLetterMessageId(CreateConsumer("orders.dead"), message);

        messageId.Should().BeNull();
    }

    private static NatsInboundConsumer CreateConsumer(string? deadLetterSubject)
    {
        return new NatsInboundConsumer(
            "ORDERS",
            "orders-worker",
            "orders.placed",
            1,
            TimeSpan.FromSeconds(30),
            5,
            1024,
            8,
            deadLetterSubject,
            new Dictionary<string, NatsInboundEndpoint>(StringComparer.Ordinal)
        );
    }

    private sealed class FakeJetStreamMessage : INatsJSMsg<byte[]>
    {
        public Action? OnTerminate { get; set; }
        public int AckCount { get; private set; }
        public int NakCount { get; private set; }
        public int TermCount { get; private set; }
        public TimeSpan? LastNakDelay { get; private set; }
        public string? LastTerminateReason { get; private set; }
        public string Subject => "orders.placed";
        public int Size => Data.Length;
        public byte[] Data { get; } = "body"u8.ToArray();
        public NatsHeaders? Headers { get; } = new ();
        public INatsConnection Connection => null!;
        public NatsJSMsgMetadata? Metadata { get; set; }
        public string ReplyTo => string.Empty;
        public NatsException? Error => null;

        public void EnsureSuccess() { }

        public ValueTask ReplyAsync(
            NatsHeaders? headers = null,
            string? replyTo = null,
            NatsPubOpts? opts = null,
            CancellationToken cancellationToken = default
        )
        {
            return ValueTask.CompletedTask;
        }

        public ValueTask AckAsync(AckOpts? opts = null, CancellationToken cancellationToken = default)
        {
            AckCount++;
            return ValueTask.CompletedTask;
        }

        public ValueTask NakAsync(AckOpts? opts = null, CancellationToken cancellationToken = default)
        {
            NakCount++;
            LastNakDelay = opts?.NakDelay;
            return ValueTask.CompletedTask;
        }

        public ValueTask AckProgressAsync(AckOpts? opts = null, CancellationToken cancellationToken = default)
        {
            return ValueTask.CompletedTask;
        }

        public ValueTask AckTerminateAsync(AckOpts? opts = null, CancellationToken cancellationToken = default)
        {
            TermCount++;
            LastTerminateReason = opts?.TerminateReason;
            OnTerminate?.Invoke();
            return ValueTask.CompletedTask;
        }
    }
}
