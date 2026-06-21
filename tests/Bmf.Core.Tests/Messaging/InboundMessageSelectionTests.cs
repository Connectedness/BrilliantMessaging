using Bmf.Core.Messaging.Inbound;
using FluentAssertions;
using Xunit;

namespace Bmf.Core.Tests.Messaging;

public sealed class InboundMessageSelectionTests
{
    [Fact]
    public void SelectionKeyComparer_UsesOrdinalSourceAndDiscriminatorComparison()
    {
        InboundEndpointSelectionKey first = new ("orders", "message.created");
        InboundEndpointSelectionKey same = new ("orders", "message.created");
        InboundEndpointSelectionKey differentCase = new ("Orders", "message.created");

        InboundEndpointSelectionKeyComparer.Instance.Equals(first, same).Should().BeTrue();
        InboundEndpointSelectionKeyComparer.Instance.GetHashCode(first)
           .Should().Be(InboundEndpointSelectionKeyComparer.Instance.GetHashCode(same));
        InboundEndpointSelectionKeyComparer.Instance.Equals(first, differentCase).Should().BeFalse();
    }

    [Fact]
    public void UnknownInboundMessageException_PreservesSourceAndDiscriminator()
    {
        UnknownInboundMessageException defaultMessage = new ("orders", "message.unknown");
        UnknownInboundMessageException customMessage = new ("billing", "message.missing", "custom");

        defaultMessage.TransportSource.Should().Be("orders");
        defaultMessage.Discriminator.Should().Be("message.unknown");
        defaultMessage.Message.Should()
           .Be("Inbound message discriminator 'message.unknown' from 'orders' is not registered.");
        customMessage.Message.Should().Be("custom");
    }
}
