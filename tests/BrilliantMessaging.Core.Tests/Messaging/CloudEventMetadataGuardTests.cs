using System;
using BrilliantMessaging.Abstractions;
using BrilliantMessaging.Core.Messaging.Outbound;
using FluentAssertions;
using Xunit;

namespace BrilliantMessaging.Core.Tests.Messaging;

public sealed class CloudEventMetadataGuardTests
{
    [Fact]
    public void From_RejectsNullCloudEvent()
    {
        var act = () => CloudEventMetadata.From(null!);

        act.Should().Throw<ArgumentNullException>().WithParameterName("cloudEvent");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void GetRequiredSource_RejectsInvalidSource(string? source)
    {
        var act = () => CloudEventsOptionsValidation.GetRequiredSource(source);

        act.Should().Throw<CloudEventMetadataException>()
           .Which.AttributeName.Should().Be(CloudEventAttributeNames.Source);
    }

    [Fact]
    public void GetRequiredSource_ReturnsValidSource()
    {
        CloudEventsOptionsValidation.GetRequiredSource("/source").Should().Be("/source");
        CloudEventsOptionsValidation.IsValidSource("https://example.test/source").Should().BeTrue();
    }

    [Theory]
    [InlineData("attributeName")]
    [InlineData("supplyInstructions")]
    public void CloudEventMetadataException_RejectsBlankConstructorArguments(string parameterName)
    {
        var attributeName = parameterName == "attributeName" ? " " : "type";
        var supplyInstructions = parameterName == "supplyInstructions" ? " " : "configure it";

        var act = () => new CloudEventMetadataException(attributeName, supplyInstructions);

        act.Should().Throw<ArgumentException>().WithParameterName(parameterName);
    }
}
