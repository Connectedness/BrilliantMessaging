using System;
using System.Linq;
using Bmf.Core.Messaging;
using Bmf.Core.Messaging.Inbound;
using Bmf.Core.Messaging.Outbound;
using Bmf.Core.Tests.Messaging.TestSupport;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Bmf.Core.Tests.Messaging;

public sealed class BmfBuilderTests
{
    [Fact]
    public void AddBmf_ReturnsBuilderOverSharedConfigurationAndRegistersCoreServicesIdempotently()
    {
        ServiceCollection services = new ();

        var first = services.AddBmf()
           .UseCloudEvents(options => options.Source = "/tests/core")
           .MapMessageContracts(contracts => contracts.Map<SampleMessage>("tests.sample"));
        var second = services.AddBmf();

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
    public void AddBmf_RejectsNullServices()
    {
        var act = () => BmfServiceCollectionExtensions.AddBmf(null!);

        act.Should().Throw<ArgumentNullException>().WithParameterName("services");
    }

    [Fact]
    public void Builder_RejectsNullConstructorArgumentsAndCallbacks()
    {
        ServiceCollection services = new ();
        MessageContractRegistryBuilder contracts = new ();
        TopologyRegistrationCatalog topologies = new ();
        BmfBuilder builder = new (services, contracts, topologies);

        var nullServices = () => new BmfBuilder(null!, contracts, topologies);
        var nullContracts = () => new BmfBuilder(services, null!, topologies);
        var nullTopologies = () => new BmfBuilder(services, contracts, null!);
        var nullMap = () => builder.MapMessageContracts(null!);
        var nullCloudEvents = () => builder.UseCloudEvents(null!);

        nullServices.Should().Throw<ArgumentNullException>().WithParameterName("services");
        nullContracts.Should().Throw<ArgumentNullException>().WithParameterName("messageContracts");
        nullTopologies.Should().Throw<ArgumentNullException>().WithParameterName("topologies");
        nullMap.Should().Throw<ArgumentNullException>().WithParameterName("configure");
        nullCloudEvents.Should().Throw<ArgumentNullException>().WithParameterName("configure");
    }
}
