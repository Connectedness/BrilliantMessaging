using System;
using System.Linq;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Bmf.Core.Messaging.Inbound;
using Bmf.Core.Messaging.Outbound;

namespace Bmf.Core.Messaging;

/// <summary>
/// Provides the <see cref="AddBmf" /> extension method that registers the BMF core services into a dependency
/// injection container.
/// </summary>
public static class BmfServiceCollectionExtensions
{
    /// <summary>
    /// Registers the BMF core services and returns a <see cref="BmfBuilder" /> for further configuration.
    /// </summary>
    /// <param name="services">The service collection to add BMF to.</param>
    /// <returns>A <see cref="BmfBuilder" /> over the shared message-contract registry and topology catalog.</returns>
    /// <remarks>
    /// Registers — idempotently, via <c>TryAdd</c> and shared get-or-add builders so repeated calls accumulate
    /// rather than duplicate — the serialization stack (<see cref="IPayloadCodec" />, <see cref="IMessageSerializer" />,
    /// <see cref="IMessageDeserializer" />), the message-contract registry, the topology registry, the default
    /// <see cref="Topology" />, the inbound inspection and acknowledgement middleware, and the
    /// <see cref="IMessagePublisher" />. The <see cref="CloudEventsOptions" /> are registered with a
    /// <c>ValidateOnStart</c> guard that fails fast when <see cref="CloudEventsOptions.Source" /> is not a valid
    /// URI-reference. Two hosted services are also added: one that provisions topologies and one that drives the
    /// inbound runtime.
    /// </remarks>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="services" /> is <see langword="null" />.</exception>
    public static BmfBuilder AddBmf(this IServiceCollection services)
    {
        if (services is null)
        {
            throw new ArgumentNullException(nameof(services));
        }

        var messageContracts = GetOrAddMessageContracts(services);
        var topologies = GetOrAddTopologies(services);

        services.AddOptions<CloudEventsOptions>()
           .Validate(
                static options => CloudEventsOptionsValidation.IsValidSource(options.Source),
                "CloudEventsOptions.Source must be a non-empty URI-reference. Configure CloudEventsOptions.Source or pass a per-call CloudEventMetadata.Source override."
            )
           .ValidateOnStart();
        services.TryAddSingleton(
            static serviceProvider => serviceProvider.GetRequiredService<IOptions<CloudEventsOptions>>().Value
        );
        services.TryAddSingleton<IMessageContractRegistry>(
            static serviceProvider => serviceProvider.GetRequiredService<MessageContractRegistryBuilder>().Build()
        );
        services.TryAddSingleton<IPayloadCodec, Utf8JsonPayloadCodec>();
        services.TryAddSingleton<CloudEventMessageSerializer>();
        services.TryAddSingleton<IMessageSerializer>(
            static serviceProvider => serviceProvider.GetRequiredService<CloudEventMessageSerializer>()
        );
        services.TryAddSingleton<PayloadCodecMessageDeserializer>();
        services.TryAddSingleton<IMessageDeserializer>(
            static serviceProvider => serviceProvider.GetRequiredService<PayloadCodecMessageDeserializer>()
        );
        services.TryAddSingleton<ITopologyRegistry, TopologyRegistry>();
        services.TryAddSingleton<CloudEventsInboundMessageInspector>();
        services.TryAddSingleton<FrameworkMessageAcknowledgementMiddleware>();
        services.TryAddSingleton<MessageDeserializationMiddleware>();
        services.TryAddSingleton<IMessagePublisher>(
            static serviceProvider => new MessagePublisher(
                serviceProvider.GetRequiredService<ITopologyRegistry>()
            )
        );
        services.TryAddSingleton<Topology>(
            static serviceProvider => serviceProvider
               .GetRequiredService<ITopologyRegistry>()
               .GetRequiredTopology(Topology.DefaultName)
        );
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, TopologyProvisioningHostedService>());
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, TopologyRuntimeHostedService>());

        return new BmfBuilder(services, messageContracts, topologies);
    }

    private static MessageContractRegistryBuilder GetOrAddMessageContracts(IServiceCollection services)
    {
        var existing = services
           .Select(static descriptor => descriptor.ImplementationInstance)
           .OfType<MessageContractRegistryBuilder>()
           .FirstOrDefault();

        if (existing is not null)
        {
            return existing;
        }

        MessageContractRegistryBuilder builder = new ();
        services.TryAddSingleton(builder);
        return builder;
    }

    private static TopologyRegistrationCatalog GetOrAddTopologies(IServiceCollection services)
    {
        var existing = services
           .Select(static descriptor => descriptor.ImplementationInstance)
           .OfType<TopologyRegistrationCatalog>()
           .FirstOrDefault();

        if (existing is not null)
        {
            return existing;
        }

        TopologyRegistrationCatalog catalog = new ();
        services.TryAddSingleton(catalog);
        return catalog;
    }
}
