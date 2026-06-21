using System;
using FluentAssertions;
using Bmf.Core.Messaging.Outbound;
using Xunit;

namespace Bmf.Core.Tests.Messaging;

public sealed class MessageDeliveryExceptionTests
{
    [Fact]
    public void Constructor_RejectsBlankTargetName()
    {
        var act = () => _ = new MessageDeliveryException(
            " ",
            MessageDeliveryFailureReason.Timeout
        );

        act.Should().Throw<ArgumentException>().WithParameterName("targetName");
    }

    [Fact]
    public void Constructor_RejectsInnerExceptionForTimeout()
    {
        var act = () => _ = new MessageDeliveryException(
            "target",
            MessageDeliveryFailureReason.Timeout,
            new InvalidOperationException()
        );

        act.Should().Throw<ArgumentException>()
           .WithParameterName("innerException")
           .WithMessage("A delivery timeout cannot provide an inner exception.*");
    }

    [Theory]
    [InlineData(MessageDeliveryFailureReason.Nacked)]
    [InlineData(MessageDeliveryFailureReason.Returned)]
    public void Constructor_RequiresInnerExceptionForBrokerFailure(MessageDeliveryFailureReason reason)
    {
        var act = () => _ = new MessageDeliveryException("target", reason);

        act.Should().Throw<ArgumentException>()
           .WithParameterName("innerException")
           .WithMessage("A delivery failure other than timeout must provide an inner exception.*");
    }
}
