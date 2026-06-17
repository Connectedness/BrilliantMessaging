using System;
using Microsoft.Extensions.DependencyInjection;
using Bmf.Core.Messaging.Outbound;

namespace Bmf.Core.Messaging;

/// <summary>
/// The fluent entry point for configuring BMF after <see cref="BmfServiceCollectionExtensions.AddBmf" />. It
/// exposes the service collection, the message-contract registry builder, and the topology catalog so that the
/// core and the transport packages can layer their registrations onto a single shared configuration.
/// </summary>
/// <remarks>
/// The builder is returned by <see cref="BmfServiceCollectionExtensions.AddBmf" /> and threaded through the
/// transport-specific extension methods (for example the RabbitMQ topology configuration). Calling
/// <see cref="BmfServiceCollectionExtensions.AddBmf" /> more than once returns a builder over the same underlying registry and catalog, so
/// configuration accumulates rather than being replaced.
/// </remarks>
public sealed class BmfBuilder
{
    /// <summary>
    /// Initializes a new instance of the <see cref="BmfBuilder" /> class.
    /// </summary>
    /// <param name="services">The service collection BMF registers its services into.</param>
    /// <param name="messageContracts">The shared message-contract registry builder.</param>
    /// <param name="topologies">The shared catalog of topology registrations.</param>
    /// <exception cref="ArgumentNullException">Thrown when any argument is <see langword="null" />.</exception>
    public BmfBuilder(
        IServiceCollection services,
        MessageContractRegistryBuilder messageContracts,
        TopologyRegistrationCatalog topologies
    )
    {
        Services = services ?? throw new ArgumentNullException(nameof(services));
        MessageContracts = messageContracts ?? throw new ArgumentNullException(nameof(messageContracts));
        Topologies = topologies ?? throw new ArgumentNullException(nameof(topologies));
    }

    /// <summary>
    /// Gets the service collection BMF registers its services into.
    /// </summary>
    public IServiceCollection Services { get; }

    /// <summary>
    /// Gets the shared message-contract registry builder used to map message types to their CloudEvents discriminators.
    /// </summary>
    public MessageContractRegistryBuilder MessageContracts { get; }

    /// <summary>
    /// Gets the shared catalog of topology registrations that transports contribute their topologies to.
    /// </summary>
    public TopologyRegistrationCatalog Topologies { get; }

    /// <summary>
    /// Maps the message contracts (type-to-discriminator mappings) used by the framework.
    /// </summary>
    /// <param name="configure">A callback that adds mappings to the shared <see cref="MessageContracts" /> builder.</param>
    /// <returns>The same <see cref="BmfBuilder" /> instance for chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="configure" /> is <see langword="null" />.</exception>
    public BmfBuilder MapMessageContracts(Action<MessageContractRegistryBuilder> configure)
    {
        if (configure is null)
        {
            throw new ArgumentNullException(nameof(configure));
        }

        configure(MessageContracts);
        return this;
    }

    /// <summary>
    /// Configures the <see cref="CloudEventsOptions" /> that supply the framework-wide CloudEvents defaults (most
    /// importantly the <see cref="CloudEventsOptions.Source" />).
    /// </summary>
    /// <param name="configure">A callback that configures the options.</param>
    /// <returns>The same <see cref="BmfBuilder" /> instance for chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="configure" /> is <see langword="null" />.</exception>
    public BmfBuilder UseCloudEvents(Action<CloudEventsOptions> configure)
    {
        if (configure is null)
        {
            throw new ArgumentNullException(nameof(configure));
        }

        Services.Configure(configure);
        return this;
    }
}
