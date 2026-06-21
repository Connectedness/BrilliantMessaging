using System;
using FluentAssertions;
using Bmf.Core.Messaging;
using Xunit;

namespace Bmf.Core.Tests.Messaging;

public sealed class TopologyValidationExceptionTests
{
    [Fact]
    public void ValidationErrors_AreSortedDeterministically()
    {
        TopologyValidationException exception = new (["zeta", "alpha", "beta"]);

        exception.ValidationErrors.Should().Equal("alpha", "beta", "zeta");
    }

    [Fact]
    public void Constructor_RequiresAtLeastOneError()
    {
        Action act = () => _ = new TopologyValidationException(Array.Empty<string>());

        act.Should().Throw<ArgumentException>();
    }
}
