using System;
using System.Collections.Generic;
using Bmf.Core.Messaging;
using FluentAssertions;
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
    public void Map_RejectsBlankCanonicalDiscriminator()
    {
        MessageContractRegistryBuilder builder = new ();

        var inbound = () => builder.Map<RegistryMessage>(" ");
        var outbound = () => builder.MapOutbound<RegistryMessage>(" ");

        inbound.Should().Throw<ArgumentException>().WithParameterName("discriminator");
        outbound.Should().Throw<ArgumentException>().WithParameterName("discriminator");
    }

    [Fact]
    public void MapBuilder_RejectsBlankAliasAndDataSchema()
    {
        MessageContractRegistryBuilder builder = new ();
        var map = builder.Map<RegistryMessage>("registry.current");

        var blankAlias = () => map.WithInboundAlias(" ");
        var blankDataSchema = () => map.WithDataSchema(" ");

        blankAlias.Should().Throw<ArgumentException>().WithParameterName("discriminator");
        blankDataSchema.Should().Throw<ArgumentException>().WithParameterName("dataSchema");
    }

    [Fact]
    public void MapBuilder_RejectsInvalidDataSchemaUriReference()
    {
        MessageContractRegistryBuilder builder = new ();
        var map = builder.Map<RegistryMessage>("registry.current");

        var act = () => map.WithDataSchema("http://[::1");

        act.Should().Throw<ArgumentException>().WithParameterName("dataSchema");
    }

    [Fact]
    public void ValidationException_RejectsNullOrEmptyErrorList()
    {
        var nullErrors = () => new MessageContractRegistryValidationException(null!);
        var emptyErrors = () => new MessageContractRegistryValidationException([]);

        nullErrors.Should().Throw<ArgumentNullException>().WithParameterName("validationErrors");
        emptyErrors.Should().Throw<ArgumentException>().WithParameterName("validationErrors");
    }

    [Fact]
    public void Constructor_CopiesMappingsAndOrdersRegisteredMessageTypes()
    {
        var discriminators = new Dictionary<Type, string>
        {
            [typeof(OtherRegistryMessage)] = "registry.other",
            [typeof(RegistryMessage)] = "registry.current"
        };
        var inbound = new Dictionary<string, Type>(StringComparer.Ordinal)
        {
            ["registry.current"] = typeof(RegistryMessage),
            ["registry.legacy"] = typeof(RegistryMessage)
        };
        var dataSchemas = new Dictionary<Type, string>
        {
            [typeof(RegistryMessage)] = "/schemas/current"
        };
        MessageContractRegistry registry = new (discriminators, inbound, dataSchemas);

        discriminators.Clear();
        inbound.Clear();
        dataSchemas.Clear();

        registry.RegisteredMessageTypes.Should().Equal(typeof(OtherRegistryMessage), typeof(RegistryMessage));
        registry.GetDiscriminator(typeof(RegistryMessage)).Should().Be("registry.current");
        registry.GetInboundDiscriminators(typeof(RegistryMessage)).Should()
           .Equal("registry.current", "registry.legacy");
        registry.TryGetDataSchema(typeof(RegistryMessage), out var dataSchema).Should().BeTrue();
        dataSchema.Should().Be("/schemas/current");
        registry.TryGetDataSchema(typeof(OtherRegistryMessage), out dataSchema).Should().BeFalse();
        dataSchema.Should().BeNull();
    }

    [Fact]
    public void Constructor_RejectsNullMappings()
    {
        var discriminators = new Dictionary<Type, string>();
        var inbound = new Dictionary<string, Type>();
        var dataSchemas = new Dictionary<Type, string>();

        var nullDiscriminators = () => new MessageContractRegistry(null!, inbound, dataSchemas);
        var nullInbound = () => new MessageContractRegistry(discriminators, null!, dataSchemas);
        var nullDataSchemas = () => new MessageContractRegistry(discriminators, inbound, null!);

        nullDiscriminators.Should().Throw<ArgumentNullException>()
           .WithParameterName("discriminatorsByMessageType");
        nullInbound.Should().Throw<ArgumentNullException>().WithParameterName("messageTypesByDiscriminator");
        nullDataSchemas.Should().Throw<ArgumentNullException>().WithParameterName("dataSchemasByMessageType");
    }

    [Fact]
    public void Lookups_RejectInvalidArgumentsAndReportMissingContracts()
    {
        MessageContractRegistry registry = new (
            new Dictionary<Type, string>
            {
                [typeof(RegistryMessage)] = "registry.current"
            },
            new Dictionary<string, Type>(StringComparer.Ordinal)
            {
                ["registry.current"] = typeof(RegistryMessage)
            },
            new Dictionary<Type, string>()
        );

        var getNullDiscriminator = () => registry.GetDiscriminator(null!);
        var tryNullDiscriminator = () => registry.TryGetDiscriminator(null!, out _);
        var getNullDataSchema = () => registry.GetDataSchema(null!);
        var getNullInbound = () => registry.GetInboundDiscriminators(null!);
        var resolveBlank = () => registry.TryResolveType(" ", out _);
        var tryNullDataSchema = () => registry.TryGetDataSchema(null!, out _);
        var missingContract = () => registry.GetDiscriminator(typeof(OtherRegistryMessage));

        getNullDiscriminator.Should().Throw<ArgumentNullException>().WithParameterName("messageType");
        tryNullDiscriminator.Should().Throw<ArgumentNullException>().WithParameterName("messageType");
        getNullDataSchema.Should().Throw<ArgumentNullException>().WithParameterName("messageType");
        getNullInbound.Should().Throw<ArgumentNullException>().WithParameterName("messageType");
        resolveBlank.Should().Throw<ArgumentException>().WithParameterName("discriminator");
        tryNullDataSchema.Should().Throw<ArgumentNullException>().WithParameterName("messageType");
        registry.TryGetDiscriminator(typeof(OtherRegistryMessage), out var discriminator).Should().BeFalse();
        discriminator.Should().BeNull();
        registry.TryResolveType("registry.missing", out var messageType).Should().BeFalse();
        messageType.Should().BeNull();
        missingContract.Should().Throw<MessageContractNotRegisteredException>()
           .Which.MessageType.Should().Be(typeof(OtherRegistryMessage));
    }

    [Fact]
    public void MessageContractNotRegisteredException_RejectsNullMessageType()
    {
        var act = () => new MessageContractNotRegisteredException(null!);

        act.Should().Throw<ArgumentNullException>().WithParameterName("messageType");
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
