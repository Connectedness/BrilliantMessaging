using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;
using Bmf.Core.Messaging.Inbound;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Bmf.Core.Tests.Messaging;

public sealed class InboundMessageInspectorChainTests
{
    [Fact]
    public async Task CompositeInboundMessageInspector_ReturnsFirstRecognizedResult()
    {
        var first = new TestInspector(null);
        var secondResult = new InboundMessageInspectionResult("tests.second", typeof(SecondMessage));
        var second = new TestInspector(secondResult);
        var third = new TestInspector(new InboundMessageInspectionResult("tests.third", typeof(ThirdMessage)));
        CompositeInboundMessageInspector inspector = new ([first, second, third]);

        var result = await inspector.InspectAsync(new TestTransportMessage(), TestContext.Current.CancellationToken);

        result.Should().BeSameAs(secondResult);
        first.InspectionCount.Should().Be(1);
        second.InspectionCount.Should().Be(1);
        third.InspectionCount.Should().Be(0);
    }

    [Fact]
    public async Task CompositeInboundMessageInspector_ReturnsNullWhenNoInspectorRecognizesDelivery()
    {
        CompositeInboundMessageInspector inspector = new ([new TestInspector(null), new TestInspector(null)]);

        var result = await inspector.InspectAsync(new TestTransportMessage(), TestContext.Current.CancellationToken);

        result.Should().BeNull();
    }

    [Fact]
    public async Task PredicateInboundMessageInspector_ReturnsResolvedResultOnlyWhenPredicateMatches()
    {
        PredicateInboundMessageInspector inspector = new (
            message => message.TryGetHeaderString("x-kind", out var value) && value == "legacy",
            "tests.legacy",
            typeof(LegacyMessage)
        );
        var recognized = new TestTransportMessage(
            new Dictionary<string, object?>
            {
                ["x-kind"] = "legacy"
            }
        );
        var unrecognized = new TestTransportMessage();

        var recognizedResult = await inspector.InspectAsync(recognized, TestContext.Current.CancellationToken);
        var unrecognizedResult = await inspector.InspectAsync(unrecognized, TestContext.Current.CancellationToken);

        recognizedResult.Should().NotBeNull();
        recognizedResult!.Discriminator.Should().Be("tests.legacy");
        recognizedResult.MessageType.Should().Be(typeof(LegacyMessage));
        recognizedResult.Message.Should().BeNull();
        unrecognizedResult.Should().BeNull();
    }

    [Fact]
    public void InboundMessageInspectorChainBuilder_BuildsOrderedServiceAndRecognizerEntries()
    {
        InboundMessageInspectorChainBuilder builder = new ();

        var entries = builder
           .CloudEvents()
           .Use<TestInspector>(ServiceLifetime.Scoped)
           .WhenHeader("x-kind", "legacy").As<LegacyMessage>("tests.legacy")
           .WhenHeader("x-present").As<SecondMessage>()
           .WhenContentType("text/plain").As<ThirdMessage>("tests.text")
           .Build();

        entries.Should().HaveCount(5);
        entries[0]
           .Should().BeOfType<ServiceInboundMessageInspectorChainEntry>().Which
           .InspectorType.Should().Be(typeof(CloudEventsInboundMessageInspector));
        entries[1]
           .Should().BeOfType<ServiceInboundMessageInspectorChainEntry>().Which
           .ServiceLifetime.Should().Be(ServiceLifetime.Scoped);
        entries[2]
           .Should().BeOfType<RecognizerInboundMessageInspectorChainEntry>().Which
           .ExplicitDiscriminator.Should().Be("tests.legacy");
        entries[3]
           .Should().BeOfType<RecognizerInboundMessageInspectorChainEntry>().Which
           .ExplicitDiscriminator.Should().BeNull();
        entries[4]
           .Should().BeOfType<RecognizerInboundMessageInspectorChainEntry>().Which
           .MessageType.Should().Be(typeof(ThirdMessage));

        var headerMatch = new TestTransportMessage(
            new Dictionary<string, object?>
            {
                ["x-kind"] = "legacy",
                ["x-present"] = "1"
            }
        );
        var contentTypeMatch = new TestTransportMessage(contentType: "TEXT/PLAIN");

        entries[2]
           .Should().BeOfType<RecognizerInboundMessageInspectorChainEntry>().Which
           .Predicate(headerMatch).Should().BeTrue();
        entries[3]
           .Should().BeOfType<RecognizerInboundMessageInspectorChainEntry>().Which
           .Predicate(headerMatch).Should().BeTrue();
        entries[4]
           .Should().BeOfType<RecognizerInboundMessageInspectorChainEntry>().Which
           .Predicate(contentTypeMatch).Should().BeTrue();
    }

    [Fact]
    public void InboundMessageRecognizerBuilder_PublicConstructor_AllowsCustomRecognizerHelpers()
    {
        InboundMessageInspectorChainBuilder builder = new ();
        InboundMessageRecognizerBuilder recognizer = new (
            builder,
            message => message.TryGetHeaderString("x-custom", out _)
        );

        recognizer.As<LegacyMessage>("tests.custom");

        var entry = builder.Build().Should().ContainSingle().Which.Should()
           .BeOfType<RecognizerInboundMessageInspectorChainEntry>().Which;
        entry.MessageType.Should().Be(typeof(LegacyMessage));
        entry.ExplicitDiscriminator.Should().Be("tests.custom");
    }

    [Fact]
    public void Constructors_AndBuilders_ValidateArguments()
    {
        var constructEmptyList = () =>
            new CompositeInboundMessageInspector(ImmutableArray<IInboundMessageInspector>.Empty);
        var constructDefaultList = () => new CompositeInboundMessageInspector(default);
        var constructListWithNullEntry = () => new CompositeInboundMessageInspector([null!]);
        var constructPredicateWithoutPredicate =
            () => new PredicateInboundMessageInspector(null!, "tests.legacy", typeof(LegacyMessage));
        var constructPredicateWithBlankDiscriminator =
            () => new PredicateInboundMessageInspector(_ => true, "  ", typeof(LegacyMessage));
        var constructPredicateWithoutMessageType =
            () => new PredicateInboundMessageInspector(_ => true, "tests.legacy", null!);
        var constructRecognizerEntryWithBlankDiscriminator =
            () => new RecognizerInboundMessageInspectorChainEntry(_ => true, typeof(LegacyMessage), "  ");
        InboundMessageInspectorChainBuilder builder = new ();
        var whenHeaderWithoutName = () => builder.WhenHeader("  ");
        var whenHeaderWithoutValue = () => builder.WhenHeader("x-kind", "  ");
        var whenContentTypeWithoutValue = () => builder.WhenContentType("  ");
        var whenWithoutPredicate = () => builder.When(null!);
        var asWithBlankExplicitDiscriminator = () => builder.WhenHeader("x-kind").As<LegacyMessage>("  ");

        constructEmptyList.Should().Throw<ArgumentNullException>().WithParameterName("inspectors");
        constructDefaultList.Should().Throw<ArgumentNullException>().WithParameterName("inspectors");
        constructListWithNullEntry.Should().Throw<ArgumentNullException>().WithParameterName("inspectors");
        constructPredicateWithoutPredicate.Should().Throw<ArgumentNullException>().WithParameterName("predicate");
        constructPredicateWithBlankDiscriminator.Should().Throw<ArgumentException>()
           .WithParameterName("discriminator");
        constructPredicateWithoutMessageType.Should().Throw<ArgumentNullException>().WithParameterName("messageType");
        constructRecognizerEntryWithBlankDiscriminator.Should().Throw<ArgumentException>()
           .WithParameterName("explicitDiscriminator");
        whenHeaderWithoutName.Should().Throw<ArgumentException>().WithParameterName("name");
        whenHeaderWithoutValue.Should().Throw<ArgumentException>().WithParameterName("value");
        whenContentTypeWithoutValue.Should().Throw<ArgumentException>().WithParameterName("value");
        whenWithoutPredicate.Should().Throw<ArgumentNullException>().WithParameterName("predicate");
        asWithBlankExplicitDiscriminator.Should().Throw<ArgumentException>()
           .WithParameterName("explicitDiscriminator");
    }

    [Fact]
    public async Task InspectAsync_RejectsNullTransportMessage()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        CompositeInboundMessageInspector composite = new ([new TestInspector(null)]);
        PredicateInboundMessageInspector predicate = new (_ => true, "tests.legacy", typeof(LegacyMessage));

        var inspectCompositeWithoutMessage = async () => await composite.InspectAsync(null!, cancellationToken);
        var inspectPredicateWithoutMessage = async () => await predicate.InspectAsync(null!, cancellationToken);

        await inspectCompositeWithoutMessage
           .Should().ThrowAsync<ArgumentNullException>().WithParameterName("transportMessage");
        await inspectPredicateWithoutMessage
           .Should().ThrowAsync<ArgumentNullException>().WithParameterName("transportMessage");
    }

    private sealed record LegacyMessage;

    private sealed record SecondMessage;

    private sealed record ThirdMessage;

    private sealed class TestInspector : IInboundMessageInspector
    {
        private readonly InboundMessageInspectionResult? _result;

        public TestInspector(InboundMessageInspectionResult? result)
        {
            _result = result;
        }

        public int InspectionCount { get; private set; }

        public ValueTask<InboundMessageInspectionResult?> InspectAsync(
            TransportMessage transportMessage,
            CancellationToken cancellationToken = default
        )
        {
            InspectionCount++;
            return new ValueTask<InboundMessageInspectionResult?>(_result);
        }
    }

    private sealed class TestTransportMessage : TransportMessage
    {
        public TestTransportMessage(
            IReadOnlyDictionary<string, object?>? headers = null,
            string? contentType = null
        )
            : base(
                "test",
                "source",
                ReadOnlyMemory<byte>.Empty,
                headers ?? new Dictionary<string, object?>(),
                contentType
            ) { }
    }
}
