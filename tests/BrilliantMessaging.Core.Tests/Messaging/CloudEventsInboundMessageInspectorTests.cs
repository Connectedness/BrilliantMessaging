using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;
using FluentAssertions;
using Xunit;

namespace BrilliantMessaging.Core.Tests.Messaging;

public sealed class CloudEventsInboundMessageInspectorTests
{
    [Fact]
    public async Task InspectAsync_ReturnsRoutingResultWithEnvelopeInItemsAndNoMessage()
    {
        const string discriminator = "tests.inspected";
        MessageContractRegistryBuilder contracts = new ();
        contracts.Map<TestMessage>(discriminator);
        CloudEventsInboundMessageInspector inspector = new (contracts.Build());
        var body = "payload"u8.ToArray();
        var transport = new TestTransportMessage(
            body,
            new Dictionary<string, object?>
            {
                ["cloudEvents:specversion"] = "1.0",
                ["cloudEvents:id"] = "8db46548-6af1-4a70-9446-81db1f26a2d3",
                ["cloudEvents:source"] = "/tests",
                ["cloudEvents:type"] = discriminator,
                ["cloudEvents:time"] = "2026-06-14T12:00:00Z"
            }
        );

        var result = await inspector.InspectAsync(transport, TestContext.Current.CancellationToken);

        result.Should().NotBeNull();
        result.Discriminator.Should().Be(discriminator);
        result.MessageType.Should().Be<TestMessage>();
        result.Message.Should().BeNull();
        result.Items.Should().NotBeNull();
        var envelope = result.Items!.GetRequiredItem(CloudEventsContextKeys.Envelope);
        envelope.Type.Should().Be(discriminator);
        envelope.Data.ToArray().Should().Equal(body);
    }

    [Fact]
    public async Task InspectAsync_ReturnsNullWhenCloudEventsTypeHeaderIsMissing()
    {
        MessageContractRegistryBuilder contracts = new ();
        contracts.Map<TestMessage>("tests.inspected");
        CloudEventsInboundMessageInspector inspector = new (contracts.Build());
        var transport = new TestTransportMessage(
            ReadOnlyMemory<byte>.Empty,
            new Dictionary<string, object?>
            {
                ["cloudEvents:specversion"] = "1.0"
            }
        );

        var result = await inspector.InspectAsync(transport, TestContext.Current.CancellationToken);

        result.Should().BeNull();
    }

    [Fact]
    public async Task InspectAsync_ThrowsUnknownInboundMessageExceptionWhenCloudEventsTypeIsUnregistered()
    {
        CloudEventsInboundMessageInspector inspector = new (new MessageContractRegistryBuilder().Build());
        var transport = new TestTransportMessage(
            ReadOnlyMemory<byte>.Empty,
            new Dictionary<string, object?>
            {
                ["cloudEvents:type"] = "tests.unknown"
            }
        );

        var act = async () => await inspector.InspectAsync(transport, TestContext.Current.CancellationToken);

        await act
           .Should().ThrowAsync<UnknownInboundMessageException>()
           .WithMessage("No inbound message contract is registered for CloudEvents type 'tests.unknown'.");
    }

    [Fact]
    public async Task InspectAsync_ThrowsUnknownInboundMessageExceptionWhenCloudEventsTypeIsPresentButEmpty()
    {
        CloudEventsInboundMessageInspector inspector = new (new MessageContractRegistryBuilder().Build());
        var transport = new TestTransportMessage(
            ReadOnlyMemory<byte>.Empty,
            new Dictionary<string, object?>
            {
                ["cloudEvents:type"] = "  "
            }
        );

        var act = async () => await inspector.InspectAsync(transport, TestContext.Current.CancellationToken);

        (await act
               .Should().ThrowAsync<UnknownInboundMessageException>()
               .WithMessage("CloudEvents attribute 'type' is present but empty."))
           .Which.TransportSource.Should().Be("source");
    }

    private sealed record TestMessage;

    private sealed class TestTransportMessage : TransportMessage
    {
        public TestTransportMessage(
            ReadOnlyMemory<byte> body,
            IReadOnlyDictionary<string, object?> headers
        )
            : base("test", "source", body, headers, contentType: "application/json") { }
    }
}
