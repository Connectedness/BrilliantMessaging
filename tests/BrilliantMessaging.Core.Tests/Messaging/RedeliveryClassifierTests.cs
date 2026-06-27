using System;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;
using FluentAssertions;
using Xunit;

namespace BrilliantMessaging.Core.Tests.Messaging;

public sealed class RedeliveryClassifierTests
{
    [Fact]
    public void RetryUnlessPoison_RetriesHandlerFailuresAndRejectsDeserializationFailures()
    {
        RedeliveryClassifier.RetryUnlessPoison
           .ShouldRetry(new InvalidOperationException("transient"))
           .Should().BeTrue();

        RedeliveryClassifier.RetryUnlessPoison
           .ShouldRetry(new MessageDeserializationException(typeof(TestMessage), new FormatException()))
           .Should().BeFalse();
    }

    [Fact]
    public void ShouldRetry_AppliesMarkerOverridesBeforePredicate()
    {
        RedeliveryClassifier classifier = new (static _ => false);

        classifier.ShouldRetry(new RetryMessageException()).Should().BeTrue();

        classifier = new RedeliveryClassifier(static _ => true);

        classifier.ShouldRetry(new RejectMessageException()).Should().BeFalse();
    }

    [Fact]
    public void RejectAll_RejectsEveryFailureIncludingRetryMarker()
    {
        RedeliveryClassifier.RejectAll
           .ShouldRetry(new InvalidOperationException("transient"))
           .Should().BeFalse();
        RedeliveryClassifier.RejectAll
           .ShouldRetry(new RetryMessageException())
           .Should().BeFalse();
    }

    [Fact]
    public void Builder_ConfiguresPredicateAndRejectsNullPredicate()
    {
        RedeliveryClassifierBuilder builder = new ();

        builder.ShouldRetry(static failure => failure is TimeoutException);
        var classifier = ((IBuildable<RedeliveryClassifier>) builder).Build();

        classifier.ShouldRetry(new TimeoutException()).Should().BeTrue();
        classifier.ShouldRetry(new InvalidOperationException()).Should().BeFalse();

        Action act = () => builder.ShouldRetry(null!);

        act.Should().Throw<ArgumentNullException>().WithParameterName("shouldRetry");
    }

    [Fact]
    public void ShouldRetry_RejectsNullFailure()
    {
        var act = () => RedeliveryClassifier.RetryUnlessPoison.ShouldRetry(null!);

        act.Should().Throw<ArgumentNullException>().WithParameterName("failure");
    }

    private sealed record TestMessage;
}
