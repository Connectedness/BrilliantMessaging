using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace BrilliantMessaging.Core.Tests.Messaging;

public sealed class InboundMessageInspectorChainCompilerTests
{
    private const string Kind = "consumer";
    private const string Name = "orders";

    [Fact]
    public void Validate_RejectsEmptyChain()
    {
        List<string> errors = [];

        InboundMessageInspectorChainCompiler.Validate([], Kind, Name, static _ => true, errors);

        errors.Should().ContainSingle().Which.Should()
           .Be("Inbound inspector chain for consumer 'orders' must contain at least one entry.");
    }

    [Fact]
    public void Validate_ReportsInvalidEntries()
    {
        List<string> errors = [];
        ImmutableArray<InboundMessageInspectorChainEntry> chain =
        [
            new ServiceInboundMessageInspectorChainEntry(typeof(string)),
            new ServiceInboundMessageInspectorChainEntry(typeof(TestInspector)),
            new RecognizerInboundMessageInspectorChainEntry(static _ => true, typeof(BaseMessage), "tests.base"),
            null!
        ];

        InboundMessageInspectorChainCompiler.Validate(chain, Kind, Name, static _ => false, errors);

        errors.Should().Equal(
            $"Inbound inspector '{typeof(string)}' for consumer 'orders' does not implement '{typeof(IInboundMessageInspector)}'.",
            $"Inbound inspector '{typeof(TestInspector)}' for consumer 'orders' is not registered.",
            "Inbound inspector chain for consumer 'orders' contains a null entry."
        );
    }

    [Fact]
    public void ResolveRecognizers_UsesExplicitDiscriminatorOrContract()
    {
        var registry = CreateRegistry();
        ImmutableArray<InboundMessageInspectorChainEntry> chain =
        [
            new ServiceInboundMessageInspectorChainEntry(typeof(TestInspector)),
            new RecognizerInboundMessageInspectorChainEntry(static _ => true, typeof(BaseMessage)),
            new RecognizerInboundMessageInspectorChainEntry(static _ => true, typeof(DerivedMessage), "tests.explicit")
        ];

        var recognizers = InboundMessageInspectorChainCompiler.ResolveRecognizers(
            chain,
            registry,
            Kind,
            Name,
            validationErrors: null
        );

        recognizers.Should().HaveCount(2);
        recognizers[0].Discriminator.Should().Be("tests.base");
        recognizers[1].Discriminator.Should().Be("tests.explicit");
    }

    [Fact]
    public void ResolveRecognizers_ReportsUnregisteredTypeOrThrowsWithoutErrorCollection()
    {
        var registry = CreateRegistry();
        ImmutableArray<InboundMessageInspectorChainEntry> chain =
        [
            new RecognizerInboundMessageInspectorChainEntry(static _ => true, typeof(DerivedMessage))
        ];
        List<string> errors = [];

        var recognizers = InboundMessageInspectorChainCompiler.ResolveRecognizers(
            chain,
            registry,
            Kind,
            Name,
            errors
        );
        var resolveAfterValidation = () => InboundMessageInspectorChainCompiler.ResolveRecognizers(
            chain,
            registry,
            Kind,
            Name,
            validationErrors: null
        );

        recognizers.Should().BeEmpty();
        errors.Should().ContainSingle().Which.Should().Be(
            $"Inbound recognizer for message '{typeof(DerivedMessage)}' on consumer 'orders' uses As<T>() but the type is not registered. Use As<T>(explicitDiscriminator) or register the contract with MessageContractRegistryBuilder.Map<T>(...)."
        );
        resolveAfterValidation.Should().Throw<MessageContractNotRegisteredException>();
    }

    [Fact]
    public void MapRecognizersToHandlers_PrefersExactTypeOverAssignableHandlers()
    {
        var baseHandler = new TestHandler(typeof(BaseMessage));
        var derivedHandler = new TestHandler(typeof(DerivedMessage));
        List<string> errors = [];

        var mappings = MapDerivedRecognizer([baseHandler, derivedHandler], errors);

        errors.Should().BeEmpty();
        mappings.Should().ContainSingle().Which.Handler.Should().BeSameAs(derivedHandler);
    }

    [Fact]
    public void MapRecognizersToHandlers_FallsBackToOnlyAssignableHandler()
    {
        var baseHandler = new TestHandler(typeof(BaseMessage));
        List<string> errors = [];

        var mappings = MapDerivedRecognizer([baseHandler, new TestHandler(typeof(UnrelatedMessage))], errors);

        errors.Should().BeEmpty();
        var mapping = mappings.Should().ContainSingle().Which;
        mapping.Handler.Should().BeSameAs(baseHandler);
        mapping.Recognizer.Discriminator.Should().Be("tests.derived");
    }

    [Fact]
    public void MapRecognizersToHandlers_ReportsMissingHandler()
    {
        List<string> errors = [];

        var mappings = MapDerivedRecognizer([new TestHandler(typeof(UnrelatedMessage))], errors);

        mappings.Should().BeEmpty();
        errors.Should().ContainSingle().Which.Should().Be(
            $"Inbound recognizer for message '{typeof(DerivedMessage)}' maps discriminator 'tests.derived' on consumer 'orders', but no handler on that consumer handles an assignable message type."
        );
    }

    [Fact]
    public void MapRecognizersToHandlers_ReportsSeveralAssignableHandlers()
    {
        List<string> errors = [];

        var mappings = MapDerivedRecognizer(
            [new TestHandler(typeof(BaseMessage)), new TestHandler(typeof(IMessageMarker))],
            errors
        );

        mappings.Should().BeEmpty();
        errors.Should().ContainSingle().Which.Should().Be(
            $"Inbound recognizer for message '{typeof(DerivedMessage)}' maps discriminator 'tests.derived' on consumer 'orders', but multiple handlers on that consumer handle an assignable message type."
        );
    }

    [Fact]
    public async Task Compile_BindsCloudEventsToEffectiveRegistryAndKeepsDeclarationOrder()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        MessageContractRegistryBuilder dialect = new ();
        dialect.Map<DerivedMessage>("tests.local");
        var effectiveRegistry = new EffectiveMessageContractRegistry(CreateRegistry(), (MessageContractRegistry) dialect.Build());
        ImmutableArray<InboundMessageInspectorChainEntry> chain =
        [
            new ServiceInboundMessageInspectorChainEntry(typeof(CloudEventsInboundMessageInspector)),
            new ServiceInboundMessageInspectorChainEntry(typeof(TestInspector)),
            new RecognizerInboundMessageInspectorChainEntry(static _ => false, typeof(BaseMessage))
        ];

        var compiled = InboundMessageInspectorChainCompiler.Compile(chain, effectiveRegistry, Kind, Name);

        compiled.Entries.Should().HaveCount(3);
        compiled.Entries[0].Should().BeOfType<CompiledInstanceInboundMessageInspectorChainEntry>().Which.Inspector
           .Should().BeOfType<CloudEventsInboundMessageInspector>();
        compiled.Entries[1].Should().BeOfType<CompiledServiceInboundMessageInspectorChainEntry>().Which.InspectorType
           .Should().Be(typeof(TestInspector));
        compiled.Entries[2].Should().BeOfType<CompiledInstanceInboundMessageInspectorChainEntry>().Which.Inspector
           .Should().BeOfType<PredicateInboundMessageInspector>();

        // No inspector is registered, so a match proves the instance entry resolved the topology-local type.
        await using var serviceProvider = new ServiceCollection().BuildServiceProvider();
        var result = await compiled.InspectAsync(serviceProvider, CreateCloudEvent("tests.local"), cancellationToken);

        result.Should().NotBeNull();
        result.MessageType.Should().Be(typeof(DerivedMessage));
    }

    [Fact]
    public void Compile_RejectsNullEntry()
    {
        ImmutableArray<InboundMessageInspectorChainEntry> chain = [null!];

        var compile = () => InboundMessageInspectorChainCompiler.Compile(chain, CreateRegistry(), Kind, Name);

        compile.Should().Throw<InvalidOperationException>()
           .WithMessage("Inbound inspector chain for consumer 'orders' contains a null entry.");
    }

    [Fact]
    public void Members_ValidateArguments()
    {
        var entry = new RecognizerInboundMessageInspectorChainEntry(static _ => true, typeof(BaseMessage));
        var recognizer = new ResolvedInboundMessageRecognizer(entry, "tests.base");

        var validateWithoutKind = () => InboundMessageInspectorChainCompiler.Validate([], null!, Name, static _ => true, []);
        var validateWithoutName = () => InboundMessageInspectorChainCompiler.Validate([], Kind, "  ", static _ => true, []);
        var validateWithoutPredicate = () => InboundMessageInspectorChainCompiler.Validate([], Kind, Name, null!, []);
        var validateWithoutErrors = () => InboundMessageInspectorChainCompiler.Validate([], Kind, Name, static _ => true, null!);
        var resolveWithoutRegistry = () => InboundMessageInspectorChainCompiler.ResolveRecognizers([], null!, Kind, Name, null);
        var mapWithoutRecognizers = () => InboundMessageInspectorChainCompiler.MapRecognizersToHandlers<TestHandler>(null!, [], static handler => handler.MessageType, Kind, Name, null);
        var mapWithoutHandlers = () => InboundMessageInspectorChainCompiler.MapRecognizersToHandlers<TestHandler>([], null!, static handler => handler.MessageType, Kind, Name, null);
        var mapWithoutSelector = () => InboundMessageInspectorChainCompiler.MapRecognizersToHandlers<TestHandler>([], [], null!, Kind, Name, null);
        var compileWithoutRegistry = () => InboundMessageInspectorChainCompiler.Compile([], null!, Kind, Name);
        var constructRecognizerWithoutEntry = () => new ResolvedInboundMessageRecognizer(null!, "tests.base");
        var constructRecognizerWithBlankDiscriminator = () => new ResolvedInboundMessageRecognizer(entry, "  ");
        var constructMappingWithoutRecognizer = () => new InboundRecognizerHandlerMapping<TestHandler>(null!, new TestHandler(typeof(BaseMessage)));
        var constructMappingWithoutHandler = () => new InboundRecognizerHandlerMapping<TestHandler>(recognizer, null!);

        validateWithoutKind.Should().Throw<ArgumentNullException>();
        validateWithoutName.Should().Throw<ArgumentException>();
        validateWithoutPredicate.Should().Throw<ArgumentNullException>().WithParameterName("isServiceRegistered");
        validateWithoutErrors.Should().Throw<ArgumentNullException>().WithParameterName("validationErrors");
        resolveWithoutRegistry.Should().Throw<ArgumentNullException>().WithParameterName("effectiveMessageContracts");
        mapWithoutRecognizers.Should().Throw<ArgumentNullException>().WithParameterName("recognizers");
        mapWithoutHandlers.Should().Throw<ArgumentNullException>().WithParameterName("handlers");
        mapWithoutSelector.Should().Throw<ArgumentNullException>().WithParameterName("getMessageType");
        compileWithoutRegistry.Should().Throw<ArgumentNullException>().WithParameterName("effectiveMessageContracts");
        constructRecognizerWithoutEntry.Should().Throw<ArgumentNullException>().WithParameterName("entry");
        constructRecognizerWithBlankDiscriminator.Should().Throw<ArgumentException>().WithParameterName("discriminator");
        constructMappingWithoutRecognizer.Should().Throw<ArgumentNullException>().WithParameterName("recognizer");
        constructMappingWithoutHandler.Should().Throw<ArgumentNullException>().WithParameterName("handler");
    }

    private static IReadOnlyList<InboundRecognizerHandlerMapping<TestHandler>> MapDerivedRecognizer(
        IReadOnlyList<TestHandler> handlers,
        ICollection<string> errors
    )
    {
        ResolvedInboundMessageRecognizer recognizer = new (
            new RecognizerInboundMessageInspectorChainEntry(static _ => true, typeof(DerivedMessage), "tests.derived"),
            "tests.derived"
        );

        return InboundMessageInspectorChainCompiler.MapRecognizersToHandlers(
            [recognizer],
            handlers,
            static handler => handler.MessageType,
            Kind,
            Name,
            errors
        );
    }

    private static IMessageContractRegistry CreateRegistry()
    {
        MessageContractRegistryBuilder builder = new ();
        builder.Map<BaseMessage>("tests.base");
        return builder.Build();
    }

    private static TestTransportMessage CreateCloudEvent(string type)
    {
        return new TestTransportMessage(
            new Dictionary<string, object?>
            {
                ["cloudEvents:specversion"] = "1.0",
                ["cloudEvents:id"] = "message-1",
                ["cloudEvents:source"] = "/tests",
                ["cloudEvents:type"] = type,
                ["cloudEvents:time"] = DateTimeOffset.UtcNow.ToString("O")
            }
        );
    }

    private interface IMessageMarker;

    private record BaseMessage;

    private sealed record DerivedMessage : BaseMessage, IMessageMarker;

    private sealed record UnrelatedMessage;

    private sealed record TestHandler(Type MessageType);

    private sealed class TestInspector : IInboundMessageInspector
    {
        public ValueTask<InboundMessageInspectionResult?> InspectAsync(
            TransportMessage transportMessage,
            CancellationToken cancellationToken = default
        )
        {
            return new ValueTask<InboundMessageInspectionResult?>((InboundMessageInspectionResult?) null);
        }
    }

    private sealed class TestTransportMessage : TransportMessage
    {
        public TestTransportMessage(IReadOnlyDictionary<string, object?> headers)
            : base("test", "source", ReadOnlyMemory<byte>.Empty, headers) { }
    }
}
