using System;
using System.Collections.Generic;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Outbound;
using BrilliantMessaging.Core.Tests.Messaging.TestSupport;
using FluentAssertions;
using Xunit;

namespace BrilliantMessaging.Core.Tests.Messaging;

public sealed class OutboundTargetContractValidatorTests
{
    [Fact]
    public void CollectValidationErrors_ReportsEveryTypedTargetWithoutCanonicalDiscriminator()
    {
        var registry = new MessageContractRegistry(
            new Dictionary<Type, string>(),
            new Dictionary<string, Type>(),
            new Dictionary<Type, string>()
        );
        KeyValuePair<string, Type>[] typedTargets =
        [
            new ("first", typeof(SampleMessage)),
            new ("second", typeof(SampleMessage))
        ];
        List<string> validationErrors = [];

        OutboundTargetContractValidator.CollectValidationErrors(registry, typedTargets, validationErrors);

        validationErrors.Should().Equal(
            "Outbound target 'first' publishes unregistered CloudEvents message type 'BrilliantMessaging.Core.Tests.Messaging.TestSupport.SampleMessage'. Register its canonical discriminator with MessageContractRegistryBuilder.Map<T>(...) or MapOutbound<T>(...).",
            "Outbound target 'second' publishes unregistered CloudEvents message type 'BrilliantMessaging.Core.Tests.Messaging.TestSupport.SampleMessage'. Register its canonical discriminator with MessageContractRegistryBuilder.Map<T>(...) or MapOutbound<T>(...)."
        );
    }

    [Fact]
    public void CollectValidationErrors_DoesNotReportRegisteredTargets()
    {
        var registry = CloudEventsTestFactory.CreateRegistry();
        KeyValuePair<string, Type>[] typedTargets = [new ("sample", typeof(SampleMessage))];
        List<string> validationErrors = [];

        OutboundTargetContractValidator.CollectValidationErrors(registry, typedTargets, validationErrors);

        validationErrors.Should().BeEmpty();
    }

    [Fact]
    public void CollectValidationErrors_RejectsNullArguments()
    {
        var registry = CloudEventsTestFactory.CreateRegistry();
        KeyValuePair<string, Type>[] typedTargets = [new ("sample", typeof(SampleMessage))];
        List<string> validationErrors = [];

        var nullRegistry = () => OutboundTargetContractValidator.CollectValidationErrors(
            null!,
            typedTargets,
            validationErrors
        );
        var nullTargets = () => OutboundTargetContractValidator.CollectValidationErrors(
            registry,
            null!,
            validationErrors
        );
        var nullErrors = () => OutboundTargetContractValidator.CollectValidationErrors(
            registry,
            typedTargets,
            null!
        );

        nullRegistry.Should().Throw<ArgumentNullException>().WithParameterName("messageContractRegistry");
        nullTargets.Should().Throw<ArgumentNullException>().WithParameterName("typedTargets");
        nullErrors.Should().Throw<ArgumentNullException>().WithParameterName("validationErrors");
    }
}
