using System;
using FluentAssertions;
using Usf.Core.Messaging.Errors;
using Xunit;

namespace Usf.Core.Tests.Messaging.Errors;

public sealed class OutboundTopologyValidationExceptionTests
{
    [Fact]
    public void ValidationErrors_AreSortedDeterministically()
    {
        OutboundTopologyValidationException exception = new (["zeta", "alpha", "beta"]);

        exception.ValidationErrors.Should().Equal("alpha", "beta", "zeta");
    }

    [Fact]
    public void Constructor_RequiresAtLeastOneError()
    {
        Action action = () => _ = new OutboundTopologyValidationException(Array.Empty<string>());

        action.Should().Throw<ArgumentException>();
    }
}
