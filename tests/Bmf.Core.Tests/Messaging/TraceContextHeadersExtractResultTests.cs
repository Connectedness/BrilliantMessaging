using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using Bmf.Core.Messaging;
using FluentAssertions;
using Xunit;

namespace Bmf.Core.Tests.Messaging;

public sealed class TraceContextHeadersExtractResultTests
{
    [Fact]
    public void Equals_IsTrueForDistinctInstancesWithEqualValues()
    {
        var left = CreateResult(baggage: Baggage(("tenant", "tenant-7"), ("region", "eu")));
        var right = CreateResult(baggage: Baggage(("tenant", "tenant-7"), ("region", "eu")));

        left.Should().NotBeSameAs(right);
        left.Should().Be(right);
    }

    [Fact]
    public void Equals_IsTrueForEmptyBaggage()
    {
        var left = CreateResult(baggage: Baggage());
        var right = CreateResult(baggage: Baggage());

        left.Should().Be(right);
    }

    [Fact]
    public void Equals_IgnoresBaggageOrder()
    {
        var left = CreateResult(baggage: Baggage(("tenant", "tenant-7"), ("region", "eu")));
        var right = CreateResult(baggage: Baggage(("region", "eu"), ("tenant", "tenant-7")));

        left.Should().Be(right);
    }

    [Fact]
    public void Equals_SupportsForeignReadOnlyDictionaryImplementations()
    {
        var dictionaryBacked = CreateResult(baggage: Baggage(("tenant", "tenant-7")));
        var readOnlyBacked = CreateResult(
            baggage: new ReadOnlyDictionary<string, string?>(
                new Dictionary<string, string?>(StringComparer.Ordinal) { ["tenant"] = "tenant-7" }
            )
        );

        dictionaryBacked.Should().Be(readOnlyBacked);
        dictionaryBacked.GetHashCode().Should().Be(readOnlyBacked.GetHashCode());
    }

    [Fact]
    public void Equals_IsFalseWhenTraceParentDiffers()
    {
        var left = CreateResult(traceParent: "00-trace-a-01");
        var right = CreateResult(traceParent: "00-trace-b-01");

        left.Should().NotBe(right);
    }

    [Fact]
    public void Equals_IsFalseWhenTraceStateDiffers()
    {
        var left = CreateResult(traceState: "vendor=a");
        var right = CreateResult(traceState: "vendor=b");

        left.Should().NotBe(right);
    }

    [Fact]
    public void Equals_IsFalseWhenBaggageValueDiffers()
    {
        var left = CreateResult(baggage: Baggage(("tenant", "tenant-7")));
        var right = CreateResult(baggage: Baggage(("tenant", "tenant-8")));

        left.Should().NotBe(right);
    }

    [Fact]
    public void Equals_IsFalseWhenBaggageKeyIsMissing()
    {
        var left = CreateResult(baggage: Baggage(("tenant", "tenant-7"), ("region", "eu")));
        var right = CreateResult(baggage: Baggage(("tenant", "tenant-7")));

        left.Should().NotBe(right);
    }

    [Fact]
    public void Equals_IsFalseWhenComparedToNull()
    {
        var result = CreateResult();

        result.Equals(null).Should().BeFalse();
    }

    [Fact]
    public void GetHashCode_IsEqualForEqualResults()
    {
        var left = CreateResult(baggage: Baggage(("tenant", "tenant-7"), ("region", "eu")));
        var right = CreateResult(baggage: Baggage(("tenant", "tenant-7"), ("region", "eu")));

        left.GetHashCode().Should().Be(right.GetHashCode());
    }

    [Fact]
    public void GetHashCode_IsIndependentOfBaggageOrder()
    {
        var left = CreateResult(baggage: Baggage(("tenant", "tenant-7"), ("region", "eu")));
        var right = CreateResult(baggage: Baggage(("region", "eu"), ("tenant", "tenant-7")));

        left.GetHashCode().Should().Be(right.GetHashCode());
    }

    [Fact]
    public void EqualityOperators_ReflectValueEquality()
    {
        var left = CreateResult(baggage: Baggage(("tenant", "tenant-7")));
        var equal = CreateResult(baggage: Baggage(("tenant", "tenant-7")));
        var different = CreateResult(baggage: Baggage(("tenant", "tenant-8")));

        (left == equal).Should().BeTrue();
        (left != equal).Should().BeFalse();
        (left == different).Should().BeFalse();
        (left != different).Should().BeTrue();
    }

    private static TraceContextHeadersExtractResult CreateResult(
        string? traceParent = "traceparent",
        string? traceState = "tracestate",
        IReadOnlyDictionary<string, string?>? baggage = null
    )
    {
        return new TraceContextHeadersExtractResult(
            traceParent,
            traceState,
            baggage ?? Baggage()
        );
    }

    private static IReadOnlyDictionary<string, string?> Baggage(params (string Key, string? Value)[] pairs)
    {
        var baggage = new Dictionary<string, string?>(StringComparer.Ordinal);

        foreach (var pair in pairs)
        {
            baggage[pair.Key] = pair.Value;
        }

        return baggage;
    }
}
