using System;
using System.Linq;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;
using BrilliantMessaging.Core.Messaging.Outbound;
using BrilliantMessaging.Core.Tests.Messaging.TestSupport;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace BrilliantMessaging.Core.Tests.Messaging;

public sealed class BrilliantMessagingBuilderTests
{
    [Fact]
    public void AddBrilliantMessaging_ReturnsBuilderOverSharedConfigurationAndRegistersCoreServicesIdempotently()
    {
        ServiceCollection services = new ();

        var first = services.AddBrilliantMessaging()
           .UseCloudEvents(options => options.Source = "/tests/core")
           .MapMessageContracts(contracts => contracts.Map<SampleMessage>("tests.sample"));
        var second = services.AddBrilliantMessaging();

        second.Services.Should().BeSameAs(services);
        second.MessageContracts.Should().BeSameAs(first.MessageContracts);
        second.Topologies.Should().BeSameAs(first.Topologies);
        second.MessageContracts.Build().GetDiscriminator(typeof(SampleMessage)).Should().Be("tests.sample");
        services.Should().ContainSingle(descriptor => descriptor.ImplementationInstance == first.MessageContracts);
        services.Should().ContainSingle(descriptor => descriptor.ImplementationInstance == first.Topologies);
        services.Should().Contain(descriptor => descriptor.ServiceType == typeof(IPayloadCodec));
        services.Should().Contain(descriptor => descriptor.ServiceType == typeof(IMessageSerializer));
        services.Should().Contain(descriptor => descriptor.ServiceType == typeof(IMessageDeserializer));
        services.Should().Contain(descriptor => descriptor.ServiceType == typeof(ITopologyRegistry));
        services.Should().Contain(descriptor => descriptor.ServiceType == typeof(IMessagePublisher));
        services.Should().Contain(descriptor => descriptor.ServiceType == typeof(CloudEventsInboundMessageInspector));
        services.Should().Contain(descriptor => descriptor.ServiceType == typeof(FrameworkMessageAcknowledgementMiddleware));
        services.Should().Contain(descriptor => descriptor.ServiceType == typeof(MessageDeserializationMiddleware));
        var hostedServiceTypes = services
           .Where(static descriptor => descriptor.ServiceType == typeof(IHostedService))
           .Select(static descriptor => descriptor.ImplementationType)
           .ToList();
        hostedServiceTypes.Should().Contain(typeof(TopologyProvisioningHostedService));
        hostedServiceTypes.Should().Contain(typeof(TopologyRuntimeHostedService));
    }

    [Fact]
    public void AddBrilliantMessaging_RejectsNullServices()
    {
        var act = () => BrilliantMessagingServiceCollectionExtensions.AddBrilliantMessaging(null!);

        act.Should().Throw<ArgumentNullException>().WithParameterName("services");
    }

    [Fact]
    public void Builder_RejectsNullConstructorArgumentsAndCallbacks()
    {
        ServiceCollection services = new ();
        MessageContractRegistryBuilder contracts = new ();
        TopologyRegistrationCatalog topologies = new ();
        BrilliantMessagingBuilder builder = new (services, contracts, topologies);

        var nullServices = () => new BrilliantMessagingBuilder(null!, contracts, topologies);
        var nullContracts = () => new BrilliantMessagingBuilder(services, null!, topologies);
        var nullTopologies = () => new BrilliantMessagingBuilder(services, contracts, null!);
        var nullMap = () => builder.MapMessageContracts(null!);
        var nullCloudEvents = () => builder.UseCloudEvents(null!);

        nullServices.Should().Throw<ArgumentNullException>().WithParameterName("services");
        nullContracts.Should().Throw<ArgumentNullException>().WithParameterName("messageContracts");
        nullTopologies.Should().Throw<ArgumentNullException>().WithParameterName("topologies");
        nullMap.Should().Throw<ArgumentNullException>().WithParameterName("configure");
        nullCloudEvents.Should().Throw<ArgumentNullException>().WithParameterName("configure");
    }
}
