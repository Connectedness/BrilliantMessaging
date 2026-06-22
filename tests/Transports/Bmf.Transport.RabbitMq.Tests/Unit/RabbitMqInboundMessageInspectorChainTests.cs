using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Bmf.Core.Messaging.Inbound;
using Bmf.Transport.RabbitMq.Inbound;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Bmf.Transport.RabbitMq.Tests.Unit;

public sealed class RabbitMqInboundMessageInspectorChainTests
{
    [Fact]
    public async Task InspectAsync_ReturnsFirstRecognizedResultAndStops()
    {
        var first = new TestInspector(null);
        var secondResult = new InboundMessageInspectionResult("tests.second", typeof(SecondMessage));
        var second = new TestInspector(secondResult);
        var third = new TestInspector(new InboundMessageInspectionResult("tests.third", typeof(ThirdMessage)));
        RabbitMqInboundMessageInspectorChain chain = new (
            [
                new RabbitMqInstanceInboundMessageInspectorChainEntry(first),
                new RabbitMqInstanceInboundMessageInspectorChainEntry(second),
                new RabbitMqInstanceInboundMessageInspectorChainEntry(third)
            ]
        );
        await using var serviceProvider = new ServiceCollection().BuildServiceProvider();

        var result = await chain.InspectAsync(
            serviceProvider,
            new TestTransportMessage(),
            TestContext.Current.CancellationToken
        );

        result.Should().BeSameAs(secondResult);
        first.InspectionCount.Should().Be(1);
        second.InspectionCount.Should().Be(1);
        third.InspectionCount.Should().Be(0);
    }

    [Fact]
    public async Task InspectAsync_ReturnsNullWhenNoEntryRecognizesDelivery()
    {
        RabbitMqInboundMessageInspectorChain chain = new (
            [
                new RabbitMqInstanceInboundMessageInspectorChainEntry(new TestInspector(null))
            ]
        );
        await using var serviceProvider = new ServiceCollection().BuildServiceProvider();

        var result = await chain.InspectAsync(
            serviceProvider,
            new TestTransportMessage(),
            TestContext.Current.CancellationToken
        );

        result.Should().BeNull();
    }

    [Fact]
    public async Task ServiceEntry_ResolvesInspectorFromPerDeliveryServiceProvider()
    {
        RabbitMqInboundMessageInspectorChain chain = new (
            [
                new RabbitMqServiceInboundMessageInspectorChainEntry(typeof(ServiceInspector))
            ]
        );
        var services = new ServiceCollection();
        services.AddSingleton<ServiceInspector>();
        await using var serviceProvider = services.BuildServiceProvider();

        var result = await chain.InspectAsync(
            serviceProvider,
            new TestTransportMessage(),
            TestContext.Current.CancellationToken
        );

        result.Should().NotBeNull();
        result!.Discriminator.Should().Be("tests.service");
        result.MessageType.Should().Be(typeof(ServiceMessage));
    }

    [Fact]
    public void Constructors_ValidateArguments()
    {
        var constructNullChain = () => new RabbitMqInboundMessageInspectorChain(null!);
        var constructChainWithNullEntry = () => new RabbitMqInboundMessageInspectorChain([null!]);
        var constructInvalidServiceEntry = () => new RabbitMqServiceInboundMessageInspectorChainEntry(typeof(string));
        var constructNullInstanceEntry = () => new RabbitMqInstanceInboundMessageInspectorChainEntry(null!);

        constructNullChain.Should().Throw<ArgumentNullException>().WithParameterName("entries");
        constructChainWithNullEntry.Should().Throw<ArgumentNullException>().WithParameterName("entries");
        constructInvalidServiceEntry.Should().Throw<ArgumentException>().WithParameterName("inspectorType");
        constructNullInstanceEntry.Should().Throw<ArgumentNullException>().WithParameterName("inspector");
    }

    [Fact]
    public async Task InspectAsync_ValidatesArguments()
    {
        RabbitMqInboundMessageInspectorChain chain = new (
            [
                new RabbitMqInstanceInboundMessageInspectorChainEntry(new TestInspector(null))
            ]
        );
        await using var serviceProvider = new ServiceCollection().BuildServiceProvider();

        var inspectWithoutProvider = async () => await chain.InspectAsync(
            null!,
            new TestTransportMessage(),
            TestContext.Current.CancellationToken
        );
        var inspectWithoutMessage = async () => await chain.InspectAsync(
            // ReSharper disable once AccessToDisposedClosure -- delegate is called before disposal
            serviceProvider,
            null!,
            TestContext.Current.CancellationToken
        );

        await inspectWithoutProvider
           .Should().ThrowAsync<ArgumentNullException>().WithParameterName("serviceProvider");
        await inspectWithoutMessage
           .Should().ThrowAsync<ArgumentNullException>().WithParameterName("transportMessage");
    }

    private sealed record SecondMessage;

    private sealed record ThirdMessage;

    private sealed record ServiceMessage;

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

    private sealed class ServiceInspector : IInboundMessageInspector
    {
        public ValueTask<InboundMessageInspectionResult?> InspectAsync(
            TransportMessage transportMessage,
            CancellationToken cancellationToken = default
        )
        {
            return new ValueTask<InboundMessageInspectionResult?>(
                new InboundMessageInspectionResult("tests.service", typeof(ServiceMessage))
            );
        }
    }

    private sealed class TestTransportMessage : TransportMessage
    {
        public TestTransportMessage()
            : base("test", "source", ReadOnlyMemory<byte>.Empty, new Dictionary<string, object?>()) { }
    }
}
