using System;
using FluentAssertions;
using Bmf.Core.Messaging;
using Xunit;

namespace Bmf.Core.Tests.Messaging;

public sealed class MessageContractRegistryTests
{
    [Fact]
    public void Build_MapsCanonicalDiscriminatorAndInboundAliasesAsymmetrically()
    {
        MessageContractRegistryBuilder builder = new ();
        builder.Map<RegistryMessage>("registry.current").WithInboundAlias("registry.legacy");

        var registry = builder.Build();

        registry.GetDiscriminator(typeof(RegistryMessage)).Should().Be("registry.current");
        registry.GetInboundDiscriminators(typeof(RegistryMessage)).Should().Equal(
            "registry.current",
            "registry.legacy"
        );
        registry.TryResolveType("registry.current", out var current).Should().BeTrue();
        current.Should().Be<RegistryMessage>();
        registry.TryResolveType("registry.legacy", out var legacy).Should().BeTrue();
        legacy.Should().Be<RegistryMessage>();
    }

    [Fact]
    public void Build_AllowsPublishOnlyMappingWithoutInboundRegistration()
    {
        MessageContractRegistryBuilder builder = new ();
        builder.MapOutbound<RegistryMessage>("registry.outbound");

        var registry = builder.Build();

        registry.GetDiscriminator(typeof(RegistryMessage)).Should().Be("registry.outbound");
        registry.GetInboundDiscriminators(typeof(RegistryMessage)).Should().BeEmpty();
        registry.TryResolveType("registry.outbound", out _).Should().BeFalse();
    }

    [Fact]
    public void Build_ReportsAllConflictingEntries()
    {
        MessageContractRegistryBuilder builder = new ();
        builder.Map<RegistryMessage>("registry.shared");
        builder.Map<RegistryMessage>("registry.second");
        builder.Map<OtherRegistryMessage>("registry.shared");
        builder.MapOutbound<OutboundRegistryMessage>("registry.outbound").WithInboundAlias("registry.legacy");

        var act = builder.Build;

        var exception = act.Should().Throw<MessageContractRegistryValidationException>().Which;
        exception.ValidationErrors.Should().Equal(
            "CloudEvents discriminator 'registry.shared' maps to multiple message types: 'Bmf.Core.Tests.Messaging.MessageContractRegistryTests+OtherRegistryMessage', 'Bmf.Core.Tests.Messaging.MessageContractRegistryTests+RegistryMessage'.",
            "Message type 'Bmf.Core.Tests.Messaging.MessageContractRegistryTests+OutboundRegistryMessage' registers inbound CloudEvents discriminators but does not accept its canonical discriminator 'registry.outbound' inbound.",
            "Message type 'Bmf.Core.Tests.Messaging.MessageContractRegistryTests+RegistryMessage' has multiple canonical CloudEvents discriminators: 'registry.second', 'registry.shared'."
        );
    }

    [Fact]
    public void Build_ReportsDuplicateInboundEntryInsteadOfFailingWhileConstructingRegistry()
    {
        MessageContractRegistryBuilder builder = new ();
        builder.Map<RegistryMessage>("registry.current").WithInboundAlias("registry.current");

        var act = builder.Build;

        var exception = act.Should().Throw<MessageContractRegistryValidationException>().Which;
        exception.ValidationErrors.Should().ContainSingle()
           .Which.Should()
           .Be(
                "Inbound CloudEvents discriminator 'registry.current' is registered multiple times for message type 'Bmf.Core.Tests.Messaging.MessageContractRegistryTests+RegistryMessage'."
            );
    }

    [Fact]
    public void EffectiveRegistry_PrefersDialectAndFallsBackToCanonicalContracts()
    {
        MessageContractRegistryBuilder canonicalBuilder = new ();
        canonicalBuilder.Map<RegistryMessage>("canonical.current").WithInboundAlias("canonical.legacy");
        canonicalBuilder.Map<OtherRegistryMessage>("canonical.other").WithDataSchema("/schemas/other");
        MessageContractRegistryBuilder dialectBuilder = new ();
        dialectBuilder.Map<RegistryMessage>("dialect.current").WithInboundAlias("dialect.legacy");
        var registry = new EffectiveMessageContractRegistry(
            canonicalBuilder.Build(),
            (MessageContractRegistry) dialectBuilder.Build()
        );

        registry.GetDiscriminator(typeof(RegistryMessage)).Should().Be("dialect.current");
        registry.GetDataSchema(typeof(RegistryMessage)).Should().BeNull();
        registry.GetDiscriminator(typeof(OtherRegistryMessage)).Should().Be("canonical.other");
        registry.GetDataSchema(typeof(OtherRegistryMessage)).Should().Be("/schemas/other");
        registry.GetInboundDiscriminators(typeof(RegistryMessage)).Should().Equal(
            "canonical.current",
            "canonical.legacy",
            "dialect.current",
            "dialect.legacy"
        );
        registry.TryGetDiscriminator(typeof(OtherRegistryMessage), out var discriminator).Should().BeTrue();
        discriminator.Should().Be("canonical.other");
        registry.TryResolveType("dialect.legacy", out var dialectType).Should().BeTrue();
        dialectType.Should().Be<RegistryMessage>();
        registry.TryResolveType("canonical.other", out var canonicalType).Should().BeTrue();
        canonicalType.Should().Be<OtherRegistryMessage>();
    }

    [Fact]
    public void EffectiveRegistry_RejectsNullConstructorAndLookupArguments()
    {
        MessageContractRegistryBuilder builder = new ();
        builder.Map<RegistryMessage>("registry.current");
        var registry = (MessageContractRegistry) builder.Build();

        var nullCanonical = () => new EffectiveMessageContractRegistry(null!, registry);
        var nullDialect = () => new EffectiveMessageContractRegistry(registry, null!);
        var nullMessageType =
            () => new EffectiveMessageContractRegistry(registry, registry).GetInboundDiscriminators(null!);

        nullCanonical.Should().Throw<ArgumentNullException>().WithParameterName("canonical");
        nullDialect.Should().Throw<ArgumentNullException>().WithParameterName("dialect");
        nullMessageType.Should().Throw<ArgumentNullException>().WithParameterName("messageType");
    }

    private sealed record RegistryMessage;

    private sealed record OtherRegistryMessage;

    private sealed record OutboundRegistryMessage;
}
