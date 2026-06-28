using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Reflection;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;
using BrilliantMessaging.Core.Messaging.Outbound;
using BrilliantMessaging.Transport.RabbitMq.Inbound;
using BrilliantMessaging.Transport.RabbitMq.Outbound;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace BrilliantMessaging.Transport.RabbitMq;

/// <summary>
/// Validates a <see cref="RabbitMqTopologyConfiguration" /> and compiles it into a single
/// <see cref="RabbitMqTopology" /> that owns one connection and exposes both outbound targets and inbound
/// endpoints through the Core <see cref="Topology" /> base.
/// </summary>
public sealed class RabbitMqTopologyCompiler
{
    private static readonly MethodInfo CreateTargetMethod = typeof(RabbitMqTopologyCompiler)
       .GetMethod(nameof(CreateTargetCore), BindingFlags.Static | BindingFlags.NonPublic)!;

    private static readonly MethodInfo CreateEndpointMethod = typeof(RabbitMqTopologyCompiler)
       .GetMethod(nameof(CreateEndpointCore), BindingFlags.Static | BindingFlags.NonPublic)!;

    private readonly IMessageContractRegistry _canonicalMessageContracts;
    private readonly Func<Type, bool> _isServiceRegistered;
    private readonly ILoggerFactory _loggerFactory;
    private readonly Func<Type, IMessageSerializer?> _resolveSerializer;

    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMqTopologyCompiler" /> class.
    /// </summary>
    /// <param name="canonicalMessageContracts">The canonical message-contract registry the topology dialect layers over.</param>
    /// <param name="loggerFactory">The factory used to create loggers for compiled components.</param>
    /// <param name="resolveSerializer">A function that resolves a serializer instance for a serializer type, or <see langword="null" /> for the default.</param>
    /// <param name="isServiceRegistered">A predicate that reports whether a handler/inspector/deserializer type is already registered.</param>
    /// <exception cref="ArgumentNullException">Thrown when any argument is <see langword="null" />.</exception>
    public RabbitMqTopologyCompiler(
        IMessageContractRegistry canonicalMessageContracts,
        ILoggerFactory loggerFactory,
        Func<Type, IMessageSerializer?> resolveSerializer,
        Func<Type, bool> isServiceRegistered
    )
    {
        _canonicalMessageContracts = canonicalMessageContracts ??
                                     throw new ArgumentNullException(nameof(canonicalMessageContracts));
        _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
        _resolveSerializer = resolveSerializer ?? throw new ArgumentNullException(nameof(resolveSerializer));
        _isServiceRegistered = isServiceRegistered ?? throw new ArgumentNullException(nameof(isServiceRegistered));
    }

    /// <summary>
    /// Validates and compiles a topology configuration into a runnable <see cref="RabbitMqTopology" />.
    /// </summary>
    /// <param name="topologyName">The name of the topology being compiled.</param>
    /// <param name="configuration">The topology configuration to compile.</param>
    /// <param name="connectionProvider">The connection provider the compiled topology will use.</param>
    /// <returns>The compiled topology.</returns>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="configuration" /> or <paramref name="connectionProvider" /> is
    /// <see langword="null" />.
    /// </exception>
    /// <exception cref="TopologyValidationException">Thrown when the configuration fails validation.</exception>
    public RabbitMqTopology Compile(
        string topologyName,
        RabbitMqTopologyConfiguration configuration,
        RabbitMqConnectionProvider connectionProvider
    )
    {
        if (configuration is null)
        {
            throw new ArgumentNullException(nameof(configuration));
        }

        if (connectionProvider is null)
        {
            throw new ArgumentNullException(nameof(connectionProvider));
        }

        var effectiveMessageContracts = CreateEffectiveMessageContracts(configuration);
        var validationErrors = Validate(configuration, effectiveMessageContracts);

        if (validationErrors.Count > 0)
        {
            throw new TopologyValidationException(validationErrors);
        }

        RabbitMqChannelSource channelSource = new (connectionProvider);

        var (outboundChannelGroups, targets, defaultTargetsByMessageType, targetsByName) = CompileOutbound(
            topologyName,
            configuration,
            effectiveMessageContracts,
            channelSource
        );
        var (inboundChannelGroups, consumers, endpoints, endpointsByName, dispatchIndex) = CompileInbound(
            topologyName,
            configuration,
            effectiveMessageContracts
        );

        var (worstCaseChannelCount, description) = CalculateChannelBudget(
            outboundChannelGroups,
            inboundChannelGroups
        );
        channelSource.SetChannelBudget(worstCaseChannelCount, description);
        LogWorstCaseChannelCount(worstCaseChannelCount, description);
        var topology = new RabbitMqTopology(
            topologyName,
            TopologyData.PrepareTopologyDataStructures(
                defaultTargetsByMessageType,
                targetsByName,
                endpointsByName
            ),
            effectiveMessageContracts,
            configuration.Exchanges,
            configuration.Queues,
            configuration.Bindings,
            outboundChannelGroups.AsReadOnly(),
            targets.AsReadOnly(),
            inboundChannelGroups.AsReadOnly(),
            consumers.AsReadOnly(),
            endpoints.AsReadOnly(),
            new ReadOnlyDictionary<InboundEndpointSelectionKey, RabbitMqInboundEndpoint>(dispatchIndex),
            BuildPipeline(configuration),
            configuration.ShutdownTimeout,
            connectionProvider,
            channelSource
        );
        WarnWhenEmpty(topology);
        return topology;
    }

    private IMessageContractRegistry CreateEffectiveMessageContracts(RabbitMqTopologyConfiguration configuration)
    {
        return configuration.MessageContractDialect is null ?
            _canonicalMessageContracts :
            new EffectiveMessageContractRegistry(_canonicalMessageContracts, configuration.MessageContractDialect);
    }

    // ----- Outbound compilation -----

    private (
        List<RabbitMqOutboundChannelGroup> ChannelGroups,
        List<OutboundTarget> Targets,
        Dictionary<Type, OutboundTarget> DefaultTargetsByMessageType,
        Dictionary<string, OutboundTarget> TargetsByName
        ) CompileOutbound(
            string topologyName,
            RabbitMqTopologyConfiguration configuration,
            IMessageContractRegistry effectiveMessageContracts,
            RabbitMqChannelSource channelSource
        )
    {
        Dictionary<string, RabbitMqOutboundChannelGroup> explicitChannelGroupsByName = new (StringComparer.Ordinal);
        List<RabbitMqOutboundChannelGroup> channelGroups = [];

        foreach (var channelGroupDefinition in OrderOutboundChannelGroups(configuration.OutboundChannelGroups))
        {
            var channelGroup = CreateChannelGroup(
                channelGroupDefinition,
                configuration.DefaultPublisherConfirmMode,
                configuration.DefaultPublisherConfirmTimeout,
                channelSource
            );
            explicitChannelGroupsByName.Add(channelGroup.Name, channelGroup);
            channelGroups.Add(channelGroup);
        }

        var exchangesByName = ToDictionary(configuration.Exchanges, static exchange => exchange.Name);
        Dictionary<Type, OutboundTarget> defaultTargetsByMessageType = new ();
        Dictionary<string, OutboundTarget> targetsByName = new (StringComparer.Ordinal);
        List<OutboundTarget> targets = [];

        foreach (var targetDefinition in OrderTargets(configuration.Targets))
        {
            var targetName = GetTargetName(targetDefinition);
            var channelGroup = ResolveOutboundChannelGroup(
                targetDefinition,
                targetName,
                explicitChannelGroupsByName,
                channelGroups,
                configuration.DefaultPublisherConfirmMode,
                configuration.DefaultPublisherConfirmTimeout,
                channelSource
            );
            var exchangeName = exchangesByName[targetDefinition.ExchangeName].Name;
            var target = CreateTarget(
                targetDefinition,
                topologyName,
                effectiveMessageContracts,
                channelGroup,
                exchangeName
            );
            targets.Add(target);

            if (string.IsNullOrWhiteSpace(targetDefinition.TargetName))
            {
                defaultTargetsByMessageType.Add(targetDefinition.MessageType, target);
            }
            else
            {
                targetsByName.Add(targetDefinition.TargetName!, target);
            }
        }

        return (channelGroups, targets, defaultTargetsByMessageType, targetsByName);
    }

    private static IEnumerable<RabbitMqOutboundChannelGroupDefinition> OrderOutboundChannelGroups(
        IReadOnlyList<RabbitMqOutboundChannelGroupDefinition> channelGroups
    )
    {
        return channelGroups.OrderBy(static channelGroup => channelGroup.Name, StringComparer.Ordinal);
    }

    private static IEnumerable<RabbitMqOutboundTargetDefinition> OrderTargets(
        IReadOnlyList<RabbitMqOutboundTargetDefinition> targets
    )
    {
        return targets
           .OrderBy(static target => target.MessageType.AssemblyQualifiedName, StringComparer.Ordinal)
           .ThenBy(static target => target.TargetName ?? string.Empty, StringComparer.Ordinal);
    }

    private static RabbitMqOutboundChannelGroup CreateChannelGroup(
        RabbitMqOutboundChannelGroupDefinition definition,
        RabbitMqPublisherConfirmMode defaultPublisherConfirmMode,
        TimeSpan? defaultPublisherConfirmTimeout,
        RabbitMqChannelSource channelSource
    )
    {
        var publisherConfirmMode = definition.PublisherConfirmMode ?? defaultPublisherConfirmMode;

        return new RabbitMqOutboundChannelGroup(
            definition.Name,
            definition.MaximumChannelCount,
            async cancellationToken => await channelSource
               .CreateChannelAsync(CreateChannelOptions(publisherConfirmMode), cancellationToken)
               .ConfigureAwait(false),
            publisherConfirmMode,
            definition.PublisherConfirmTimeout ?? defaultPublisherConfirmTimeout
        );
    }

    private static CreateChannelOptions? CreateChannelOptions(RabbitMqPublisherConfirmMode publisherConfirmMode)
    {
        return publisherConfirmMode switch
        {
            RabbitMqPublisherConfirmMode.FireAndForget => null,
            RabbitMqPublisherConfirmMode.Confirms => new CreateChannelOptions(
                publisherConfirmationsEnabled: true,
                publisherConfirmationTrackingEnabled: true
            ),
            _ => throw new ArgumentOutOfRangeException(
                nameof(publisherConfirmMode),
                publisherConfirmMode,
                "Unsupported publisher confirm mode."
            )
        };
    }

    private static RabbitMqOutboundChannelGroup ResolveOutboundChannelGroup(
        RabbitMqOutboundTargetDefinition targetDefinition,
        string targetName,
        IReadOnlyDictionary<string, RabbitMqOutboundChannelGroup> explicitChannelGroupsByName,
        ICollection<RabbitMqOutboundChannelGroup> channelGroups,
        RabbitMqPublisherConfirmMode defaultPublisherConfirmMode,
        TimeSpan? defaultPublisherConfirmTimeout,
        RabbitMqChannelSource channelSource
    )
    {
        if (!string.IsNullOrWhiteSpace(targetDefinition.ChannelGroupName))
        {
            return explicitChannelGroupsByName[targetDefinition.ChannelGroupName!];
        }

        var implicitChannelGroup = CreateChannelGroup(
            new RabbitMqOutboundChannelGroupDefinition(
                $"{RabbitMqOutboundChannelGroupDefinition.ReservedImplicitNamePrefix}{channelGroups.Count}:{targetName}",
                1
            ),
            defaultPublisherConfirmMode,
            defaultPublisherConfirmTimeout,
            channelSource
        );
        channelGroups.Add(implicitChannelGroup);
        return implicitChannelGroup;
    }

    private OutboundTarget CreateTarget(
        RabbitMqOutboundTargetDefinition targetDefinition,
        string topologyName,
        IMessageContractRegistry messageContractRegistry,
        RabbitMqOutboundChannelGroup channelGroup,
        string exchangeName
    )
    {
        var serializer = _resolveSerializer(targetDefinition.SerializerType!) ??
                         throw new InvalidOperationException(
                             $"Serializer '{targetDefinition.SerializerType}' is not registered."
                         );
        var closedMethod = CreateTargetMethod.MakeGenericMethod(targetDefinition.MessageType);
        return (OutboundTarget) closedMethod.Invoke(
            null,
            [targetDefinition, serializer, messageContractRegistry, topologyName, channelGroup, exchangeName]
        )!;
    }

    private static OutboundTarget CreateTargetCore<TMessage>(
        RabbitMqOutboundTargetDefinition targetDefinition,
        IMessageSerializer serializer,
        IMessageContractRegistry messageContractRegistry,
        string topologyName,
        RabbitMqOutboundChannelGroup channelGroup,
        string exchangeName
    )
    {
        var targetName = GetTargetName(targetDefinition);

        return targetDefinition switch
        {
            RabbitMqFanoutOutboundTargetDefinition fanoutTarget => new RabbitMqFanoutOutboundTarget<TMessage>(
                targetName,
                serializer,
                messageContractRegistry,
                topologyName,
                channelGroup,
                exchangeName,
                fanoutTarget.IsMandatory
            ),
            RabbitMqDirectOutboundTargetDefinition directTarget => new RabbitMqDirectOutboundTarget<TMessage>(
                targetName,
                serializer,
                messageContractRegistry,
                topologyName,
                channelGroup,
                exchangeName,
                directTarget.IsMandatory,
                directTarget.RoutingKey,
                CreateRoutingKeyFactory<TMessage>(directTarget)
            ),
            RabbitMqTopicOutboundTargetDefinition topicTarget => new RabbitMqTopicOutboundTarget<TMessage>(
                targetName,
                serializer,
                messageContractRegistry,
                topologyName,
                channelGroup,
                exchangeName,
                topicTarget.IsMandatory,
                topicTarget.RoutingKey,
                CreateRoutingKeyFactory<TMessage>(topicTarget)
            ),
            RabbitMqHeadersOutboundTargetDefinition headersTarget => new RabbitMqHeadersOutboundTarget<TMessage>(
                targetName,
                serializer,
                messageContractRegistry,
                topologyName,
                channelGroup,
                exchangeName,
                headersTarget.IsMandatory,
                headersTarget.Headers
            ),
            _ => throw new ArgumentOutOfRangeException(
                nameof(targetDefinition),
                targetDefinition,
                "Unsupported RabbitMQ outbound target."
            )
        };
    }

    private static Func<TMessage, string>? CreateRoutingKeyFactory<TMessage>(
        RabbitMqRoutingKeyOutboundTargetDefinition targetDefinition
    )
    {
        if (targetDefinition.RoutingKeyFactory is null)
        {
            return null;
        }

        if (targetDefinition.RoutingKeyFactory is Func<TMessage, string> typedRoutingKeyFactory)
        {
            return typedRoutingKeyFactory;
        }

        throw new ArgumentException("A routing-key target must provide a routing-key factory for its message type.");
    }

    // ----- Inbound compilation -----

    private (
        List<RabbitMqInboundChannelGroup> ChannelGroups,
        List<RabbitMqInboundConsumer> Consumers,
        List<RabbitMqInboundEndpoint> Endpoints,
        Dictionary<string, InboundEndpoint> EndpointsByName,
        Dictionary<InboundEndpointSelectionKey, RabbitMqInboundEndpoint> DispatchIndex
        ) CompileInbound(
            string topologyName,
            RabbitMqTopologyConfiguration configuration,
            IMessageContractRegistry effectiveMessageContracts
        )
    {
        Dictionary<string, RabbitMqInboundChannelGroup> explicitChannelGroupsByName = new (StringComparer.Ordinal);
        List<RabbitMqInboundChannelGroup> channelGroups = [];
        Dictionary<string, InboundEndpoint> endpointsByName = new (StringComparer.Ordinal);
        Dictionary<InboundEndpointSelectionKey, RabbitMqInboundEndpoint> dispatchIndex =
            new (InboundEndpointSelectionKeyComparer.Instance);
        var queuesByName = ToDictionary(configuration.Queues, static queue => queue.Name);
        List<RabbitMqInboundConsumer> consumers = [];
        List<RabbitMqInboundEndpoint> endpoints = [];

        foreach (var channelGroupDefinition in OrderInboundChannelGroups(configuration.InboundChannelGroups))
        {
            var channelGroup = CreateInboundChannelGroup(channelGroupDefinition);
            explicitChannelGroupsByName.Add(channelGroup.Name, channelGroup);
            channelGroups.Add(channelGroup);
        }

        foreach (var consumerDefinition in OrderConsumers(configuration.Consumers))
        {
            var channelGroup = ResolveInboundChannelGroup(
                consumerDefinition,
                explicitChannelGroupsByName,
                channelGroups
            );
            var queueType = ResolveConsumerQueueType(consumerDefinition, queuesByName, validationErrors: null);
            var endpointPlans = CreateInboundEndpointPlans(
                consumerDefinition,
                effectiveMessageContracts,
                GetDefaultRedeliveryClassifier(queueType)
            );
            List<RabbitMqInboundEndpoint> consumerEndpoints = [];

            foreach (var endpointPlan in endpointPlans)
            {
                var endpointName = endpointPlan.Handler.EndpointName ??
                                   $"{consumerDefinition.QueueName}:{endpointPlan.PrimaryDiscriminator}";
                var endpoint = CreateEndpoint(
                    endpointPlan.Handler,
                    topologyName,
                    endpointName,
                    endpointPlan.PrimaryDiscriminator,
                    endpointPlan.RedeliveryClassifier
                );

                endpoints.Add(endpoint);
                consumerEndpoints.Add(endpoint);
                endpointsByName.Add(endpoint.Name, endpoint);

                foreach (var discriminator in endpointPlan.DispatchDiscriminators)
                {
                    var dispatchKey = new InboundEndpointSelectionKey(consumerDefinition.QueueName, discriminator);
                    dispatchIndex.Add(dispatchKey, endpoint);
                }
            }

            consumers.Add(
                new RabbitMqInboundConsumer(
                    consumerDefinition.QueueName,
                    CreateInspectorChain(consumerDefinition, effectiveMessageContracts),
                    consumerDefinition.CopyBody,
                    channelGroup,
                    consumerEndpoints.AsReadOnly()
                )
            );
        }

        return (channelGroups, consumers, endpoints, endpointsByName, dispatchIndex);
    }

    private static IReadOnlyList<InboundEndpointPlan> CreateInboundEndpointPlans(
        RabbitMqInboundConsumerDefinition consumerDefinition,
        IMessageContractRegistry effectiveMessageContracts,
        RedeliveryClassifier defaultRedeliveryClassifier
    )
    {
        var orderedHandlers = OrderHandlers(consumerDefinition.Handlers).ToArray();
        var recognizers = ResolveRecognizers(
            consumerDefinition,
            effectiveMessageContracts,
            validationErrors: null
        );
        var recognizerMappings = MapRecognizersToHandlers(
            consumerDefinition.QueueName,
            orderedHandlers,
            recognizers,
            validationErrors: null
        );
        List<InboundEndpointPlan> endpointPlans = [];

        foreach (var handlerDefinition in orderedHandlers)
        {
            var hasContract = TryGetMessageContractDiscriminators(
                effectiveMessageContracts,
                handlerDefinition.MessageType,
                out var canonicalDiscriminator,
                out var inboundDiscriminators
            );
            var recognizerDiscriminators = recognizerMappings
               .Where(mapping => ReferenceEquals(mapping.Handler, handlerDefinition))
               .Select(static mapping => mapping.Recognizer.Discriminator)
               .ToArray();
            var dispatchDiscriminators = inboundDiscriminators
               .Concat(recognizerDiscriminators)
               .Distinct(StringComparer.Ordinal)
               .ToArray();

            var primaryDiscriminator = hasContract && inboundDiscriminators.Count > 0 ?
                canonicalDiscriminator! :
                recognizerDiscriminators.First();

            endpointPlans.Add(
                new InboundEndpointPlan(
                    handlerDefinition,
                    primaryDiscriminator,
                    dispatchDiscriminators,
                    handlerDefinition.RedeliveryClassifier ??
                    consumerDefinition.RedeliveryClassifier ??
                    defaultRedeliveryClassifier
                )
            );
        }

        return endpointPlans;
    }

    private static RabbitMqInboundMessageInspectorChain CreateInspectorChain(
        RabbitMqInboundConsumerDefinition consumerDefinition,
        IMessageContractRegistry effectiveMessageContracts
    )
    {
        List<RabbitMqInboundMessageInspectorChainEntry> entries = [];

        foreach (var entry in consumerDefinition.InspectorChain)
        {
            switch (entry)
            {
                case ServiceInboundMessageInspectorChainEntry serviceEntry:
                    entries.Add(new RabbitMqServiceInboundMessageInspectorChainEntry(serviceEntry.InspectorType));
                    break;
                case RecognizerInboundMessageInspectorChainEntry recognizerEntry:
                    entries.Add(
                        new RabbitMqInstanceInboundMessageInspectorChainEntry(
                            new PredicateInboundMessageInspector(
                                recognizerEntry.Predicate,
                                ResolveRecognizerDiscriminator(recognizerEntry, effectiveMessageContracts),
                                recognizerEntry.MessageType
                            )
                        )
                    );
                    break;
                case null:
                    throw new InvalidOperationException(
                        $"Inbound inspector chain for queue '{consumerDefinition.QueueName}' contains a null entry."
                    );
                default:
                    throw new InvalidOperationException(
                        $"Inbound inspector chain entry '{GetTypeName(entry.GetType())}' for queue '{consumerDefinition.QueueName}' is not supported."
                    );
            }
        }

        return new RabbitMqInboundMessageInspectorChain(entries.AsReadOnly());
    }

    private static IEnumerable<RabbitMqInboundChannelGroupDefinition> OrderInboundChannelGroups(
        IReadOnlyList<RabbitMqInboundChannelGroupDefinition> channelGroups
    )
    {
        return channelGroups.OrderBy(static channelGroup => channelGroup.Name, StringComparer.Ordinal);
    }

    private static IEnumerable<RabbitMqInboundConsumerDefinition> OrderConsumers(
        IReadOnlyList<RabbitMqInboundConsumerDefinition> consumers
    )
    {
        return consumers.OrderBy(static consumer => consumer.QueueName, StringComparer.Ordinal);
    }

    private static IEnumerable<RabbitMqInboundHandlerDefinition> OrderHandlers(
        IReadOnlyList<RabbitMqInboundHandlerDefinition> handlers
    )
    {
        return handlers
           .OrderBy(static handler => handler.MessageType.AssemblyQualifiedName, StringComparer.Ordinal)
           .ThenBy(static handler => handler.EndpointName ?? string.Empty, StringComparer.Ordinal);
    }

    private static RabbitMqInboundChannelGroup CreateInboundChannelGroup(
        RabbitMqInboundChannelGroupDefinition definition
    )
    {
        return new RabbitMqInboundChannelGroup(
            definition.Name,
            definition.MaximumChannelCount,
            definition.PrefetchCount,
            definition.ConsumerDispatchConcurrency
        );
    }

    private static RabbitMqInboundChannelGroup ResolveInboundChannelGroup(
        RabbitMqInboundConsumerDefinition consumerDefinition,
        IReadOnlyDictionary<string, RabbitMqInboundChannelGroup> explicitChannelGroupsByName,
        ICollection<RabbitMqInboundChannelGroup> channelGroups
    )
    {
        if (!string.IsNullOrWhiteSpace(consumerDefinition.ChannelGroupName))
        {
            return explicitChannelGroupsByName[consumerDefinition.ChannelGroupName!];
        }

        var implicitChannelGroup = new RabbitMqInboundChannelGroup(
            $"{RabbitMqInboundChannelGroupDefinition.ReservedImplicitNamePrefix}{channelGroups.Count}:{consumerDefinition.QueueName}",
            consumerDefinition.ChannelCount,
            consumerDefinition.PrefetchCount,
            consumerDefinition.ConsumerDispatchConcurrency
        );
        channelGroups.Add(implicitChannelGroup);
        return implicitChannelGroup;
    }

    private static RabbitMqInboundEndpoint CreateEndpoint(
        RabbitMqInboundHandlerDefinition handlerDefinition,
        string topologyName,
        string endpointName,
        string discriminator,
        RedeliveryClassifier redeliveryClassifier
    )
    {
        var closedMethod = CreateEndpointMethod.MakeGenericMethod(handlerDefinition.MessageType);
        return (RabbitMqInboundEndpoint) closedMethod.Invoke(
            null,
            [handlerDefinition, topologyName, endpointName, discriminator, redeliveryClassifier]
        )!;
    }

    private static RabbitMqInboundEndpoint CreateEndpointCore<TMessage>(
        RabbitMqInboundHandlerDefinition handlerDefinition,
        string topologyName,
        string endpointName,
        string discriminator,
        RedeliveryClassifier redeliveryClassifier
    )
    {
        return new RabbitMqInboundEndpoint<TMessage>(
            endpointName,
            topologyName,
            handlerDefinition.HandlerType,
            handlerDefinition.DeserializerType,
            discriminator,
            handlerDefinition.HandlerInvocation,
            handlerDefinition.AckMode,
            redeliveryClassifier
        );
    }

    private static MessageDelegate BuildPipeline(RabbitMqTopologyConfiguration configuration)
    {
        MessagePipelineBuilder pipeline = new ();
        pipeline.UseMiddleware<InboundDiagnosticsMiddleware>();
        pipeline.UseMiddleware<FrameworkMessageAcknowledgementMiddleware>();
        pipeline.Use(
            next => async context =>
            {
                var middleware = (IMessageMiddleware) context.Services.GetRequiredService(
                    configuration.DeserializationMiddlewareType
                );
                await middleware.InvokeAsync(context, next).ConfigureAwait(false);
            }
        );
        configuration.ConfigurePipeline?.Invoke(pipeline);
        return pipeline.Build(static context => context.Endpoint.InvokeHandlerAsync(context));
    }

    // ----- Channel budget -----

    private static (int WorstCaseChannelCount, string Description) CalculateChannelBudget(
        IReadOnlyList<RabbitMqOutboundChannelGroup> outboundChannelGroups,
        IReadOnlyList<RabbitMqInboundChannelGroup> inboundChannelGroups
    )
    {
        List<(string Name, int MaximumChannelCount)> all = [];
        all.AddRange(outboundChannelGroups.Select(static group => (group.Name, group.MaximumChannelCount)));
        all.AddRange(inboundChannelGroups.Select(static group => (group.Name, group.MaximumChannelCount)));

        if (all.Count == 0)
        {
            return (0, "no channel groups configured");
        }

        var worstCaseChannelCount = all.Sum(static group => group.MaximumChannelCount);

        if (all.Count == 1)
        {
            return (worstCaseChannelCount, $"channel group '{all[0].Name}' max {all[0].MaximumChannelCount}");
        }

        return (worstCaseChannelCount, $"{all.Count} channel groups");
    }

    // ----- Validation -----

    private List<string> Validate(
        RabbitMqTopologyConfiguration configuration,
        IMessageContractRegistry effectiveMessageContracts
    )
    {
        List<string> validationErrors = [];

        if (configuration.CreateConnectionFactory is null)
        {
            validationErrors.Add("A RabbitMQ connection factory must be configured.");
        }

        validationErrors.AddRange(
            FindDuplicateNames(configuration.Exchanges.Select(static exchange => exchange.Name), "exchange")
        );
        validationErrors.AddRange(
            FindDuplicateNames(configuration.Queues.Select(static queue => queue.Name), "queue")
        );
        validationErrors.AddRange(
            FindDuplicateNames(configuration.OutboundChannelGroups.Select(static group => group.Name), "channel group")
        );
        validationErrors.AddRange(
            FindDuplicateNames(
                configuration.Targets.Where(static target => !string.IsNullOrWhiteSpace(target.TargetName))
                   .Select(static target => target.TargetName!),
                "target"
            )
        );

        var exchangesByName = ToDictionary(configuration.Exchanges, static exchange => exchange.Name);
        var queuesByName = ToDictionary(configuration.Queues, static queue => queue.Name);
        var outboundChannelGroupsByName = ToDictionary(
            configuration.OutboundChannelGroups,
            static group => group.Name
        );
        var inboundChannelGroupsByName = ToDictionary(
            configuration.InboundChannelGroups,
            static group => group.Name
        );
        var consumerQueueNames = new HashSet<string>(
            configuration.Consumers.Select(static consumer => consumer.QueueName),
            StringComparer.Ordinal
        );

        ValidateExchangeDefinitions(configuration.Exchanges, validationErrors);
        ValidateQueueDefinitions(configuration.Queues, consumerQueueNames, validationErrors);
        ValidateBindings(configuration.Bindings, exchangesByName, queuesByName, validationErrors);

        // Outbound validation.
        ValidateDefaultPublisherConfirmConfiguration(configuration, validationErrors);
        ValidateOutboundChannelGroupDefinitions(configuration.OutboundChannelGroups, validationErrors);
        ValidateOutboundChannelGroupUsage(configuration.OutboundChannelGroups, configuration.Targets, validationErrors);
        ValidateTargets(
            configuration.Targets,
            exchangesByName,
            outboundChannelGroupsByName,
            configuration.DefaultPublisherConfirmMode,
            validationErrors
        );
        ValidateMessageContracts(effectiveMessageContracts, configuration.Targets, validationErrors);
        ValidateMessageContractDialect(configuration.MessageContractDialect, configuration.Targets, validationErrors);

        // Inbound validation.
        ValidateInboundChannelGroupDefinitions(configuration.InboundChannelGroups, validationErrors);
        ValidateInboundChannelGroupUsage(configuration.InboundChannelGroups, configuration.Consumers, validationErrors);
        ValidatePipeline(configuration, validationErrors);
        ValidateConsumers(
            configuration.Consumers,
            queuesByName,
            inboundChannelGroupsByName,
            effectiveMessageContracts,
            validationErrors
        );

        return validationErrors;
    }

    private static void ValidateExchangeDefinitions(
        IReadOnlyList<RabbitMqExchangeDefinition> exchanges,
        ICollection<string> validationErrors
    )
    {
        foreach (var exchange in exchanges.OrderBy(static exchange => exchange.Name, StringComparer.Ordinal))
        {
            if (!Enum.IsDefined(typeof(RabbitMqDeclareMode), exchange.DeclareMode))
            {
                validationErrors.Add(
                    $"Exchange '{exchange.Name}' uses unsupported declare mode '{exchange.DeclareMode}'."
                );
            }

            if (string.Equals(exchange.Type, "internal", StringComparison.OrdinalIgnoreCase))
            {
                validationErrors.Add($"Exchange '{exchange.Name}' uses unsupported exchange type 'internal'.");
            }
        }
    }

    private static void ValidateQueueDefinitions(
        IReadOnlyList<RabbitMqQueueDefinition> queues,
        HashSet<string> consumerQueueNames,
        ICollection<string> validationErrors
    )
    {
        foreach (var queue in queues.OrderBy(static queue => queue.Name, StringComparer.Ordinal))
        {
            if (!Enum.IsDefined(typeof(RabbitMqDeclareMode), queue.DeclareMode))
            {
                validationErrors.Add($"Queue '{queue.Name}' uses unsupported declare mode '{queue.DeclareMode}'.");
            }

            if (GetDeclaredQueueType(queue) == RabbitMqQueueType.Quorum)
            {
                List<string> classicOnlyFlags = [];

                if (!queue.Durable)
                {
                    classicOnlyFlags.Add("durable=false");
                }

                if (queue.Exclusive)
                {
                    classicOnlyFlags.Add("exclusive=true");
                }

                if (queue.AutoDelete)
                {
                    classicOnlyFlags.Add("autoDelete=true");
                }

                if (classicOnlyFlags.Count > 0)
                {
                    validationErrors.Add(
                        $"Queue '{queue.Name}' is configured as a quorum queue but sets classic-only flags ({string.Join(", ", classicOnlyFlags)}). Quorum queues must be durable, non-exclusive, and non-auto-delete; call AsClassicQueue() to use those flags."
                    );
                }
            }

            // Queue-type-incompatible knob guards run for queues that no consumer references. Queues with
            // consumers are guarded in ValidateConsumers using the consumer-resolved effective queue type,
            // which accounts for an explicit QueueType(Quorum) on a passive queue. Delete-mode queues are
            // being removed, so their knobs are irrelevant.
            if (queue.DeclareMode != RabbitMqDeclareMode.Delete && !consumerQueueNames.Contains(queue.Name))
            {
                ValidateQueueKnobs(queue, GetDeclaredQueueType(queue), validationErrors);
            }
        }
    }

    private static void ValidateQueueKnobs(
        RabbitMqQueueDefinition queue,
        RabbitMqQueueType effectiveQueueType,
        ICollection<string> validationErrors
    )
    {
        // (a) Quorum-only args on Classic/Unknown — these args require a quorum queue.
        if (effectiveQueueType != RabbitMqQueueType.Quorum)
        {
            string[] quorumOnlyArgs =
            [
                "x-delivery-limit",
                "x-delayed-retry-type",
                "x-delayed-retry-min",
                "x-delayed-retry-max",
                "x-dead-letter-strategy",
                "x-queue-leader-locator",
                "x-quorum-initial-group-size",
                "x-consumer-timeout"
            ];

            foreach (var argName in quorumOnlyArgs)
            {
                if (queue.Arguments.ContainsKey(argName))
                {
                    validationErrors.Add(
                        $"Queue '{queue.Name}' configures '{argName}' but the effective queue type is '{FormatQueueType(effectiveQueueType)}'. This argument requires a quorum queue; call AsQuorumQueue() on the queue declaration or QueueType(RabbitMqQueueType.Quorum) on the consumer."
                    );
                }
            }
        }

        // (b) x-overflow = reject-publish-dlx on Quorum — quorum queues do not support reject-publish-dlx.
        if (effectiveQueueType == RabbitMqQueueType.Quorum &&
            queue.Arguments.TryGetValue("x-overflow", out var overflow) &&
            overflow is "reject-publish-dlx")
        {
            validationErrors.Add(
                $"Queue '{queue.Name}' configures 'x-overflow=reject-publish-dlx' but the effective queue type is 'quorum'. Quorum queues do not support reject-publish-dlx; use DropHead or RejectPublish, or call AsClassicQueue() to use reject-publish-dlx."
            );
        }

        // (c) x-max-priority on Quorum — quorum queues silently ignore this arg and always use the full 0-31
        // priority range, so the user's configured range would not take effect.
        if (effectiveQueueType == RabbitMqQueueType.Quorum &&
            queue.Arguments.ContainsKey("x-max-priority"))
        {
            validationErrors.Add(
                $"Queue '{queue.Name}' configures 'x-max-priority' but the effective queue type is 'quorum'. Quorum queues silently ignore x-max-priority and always use the full 0-31 priority range; call AsClassicQueue() to control the priority range."
            );
        }
    }

    private static void ValidateOutboundChannelGroupDefinitions(
        IReadOnlyList<RabbitMqOutboundChannelGroupDefinition> channelGroups,
        ICollection<string> validationErrors
    )
    {
        foreach (var channelGroup in channelGroups.OrderBy(static group => group.Name, StringComparer.Ordinal))
        {
            if (channelGroup.MaximumChannelCount < 1)
            {
                validationErrors.Add(
                    $"Channel group '{channelGroup.Name}' maximum channel count must be greater than zero."
                );
            }

            if (channelGroup.Name.StartsWith(
                    RabbitMqOutboundChannelGroupDefinition.ReservedImplicitNamePrefix,
                    StringComparison.Ordinal
                ))
            {
                validationErrors.Add(
                    $"Channel group '{channelGroup.Name}' uses reserved name prefix '{RabbitMqOutboundChannelGroupDefinition.ReservedImplicitNamePrefix}'."
                );
            }

            if (channelGroup.PublisherConfirmMode is not null &&
                !Enum.IsDefined(typeof(RabbitMqPublisherConfirmMode), channelGroup.PublisherConfirmMode.Value))
            {
                validationErrors.Add(
                    $"Channel group '{channelGroup.Name}' uses unsupported publisher confirm mode '{channelGroup.PublisherConfirmMode}'."
                );
            }

            if (channelGroup.PublisherConfirmTimeout is not null &&
                !RabbitMqPublisherConfirmDefaults.IsValidTimeout(channelGroup.PublisherConfirmTimeout.Value))
            {
                validationErrors.Add(
                    $"Channel group '{channelGroup.Name}' publisher confirm timeout must be finite and greater than zero."
                );
            }
        }
    }

    private static void ValidateDefaultPublisherConfirmConfiguration(
        RabbitMqTopologyConfiguration configuration,
        ICollection<string> validationErrors
    )
    {
        if (!Enum.IsDefined(typeof(RabbitMqPublisherConfirmMode), configuration.DefaultPublisherConfirmMode))
        {
            validationErrors.Add(
                $"RabbitMQ outbound topology uses unsupported default publisher confirm mode '{configuration.DefaultPublisherConfirmMode}'."
            );
        }

        if (configuration.DefaultPublisherConfirmTimeout is not null &&
            !RabbitMqPublisherConfirmDefaults.IsValidTimeout(configuration.DefaultPublisherConfirmTimeout.Value))
        {
            validationErrors.Add(
                "RabbitMQ outbound topology publisher confirm timeout must be finite and greater than zero."
            );
        }
    }

    private static void ValidateOutboundChannelGroupUsage(
        IReadOnlyList<RabbitMqOutboundChannelGroupDefinition> channelGroups,
        IReadOnlyList<RabbitMqOutboundTargetDefinition> targets,
        ICollection<string> validationErrors
    )
    {
        var referencedChannelGroups = new HashSet<string>(
            targets
               .Where(static target => !string.IsNullOrWhiteSpace(target.ChannelGroupName))
               .Select(static target => target.ChannelGroupName!),
            StringComparer.Ordinal
        );

        foreach (var channelGroupName in channelGroups
                    .Select(static group => group.Name)
                    .Distinct(StringComparer.Ordinal)
                    .OrderBy(static name => name, StringComparer.Ordinal))
        {
            if (!referencedChannelGroups.Contains(channelGroupName))
            {
                validationErrors.Add(
                    $"Channel group '{channelGroupName}' is configured but no outbound target references it."
                );
            }
        }
    }

    private void ValidateTargets(
        IReadOnlyList<RabbitMqOutboundTargetDefinition> targets,
        IReadOnlyDictionary<string, RabbitMqExchangeDefinition> exchangesByName,
        IReadOnlyDictionary<string, RabbitMqOutboundChannelGroupDefinition> channelGroupsByName,
        RabbitMqPublisherConfirmMode defaultPublisherConfirmMode,
        ICollection<string> validationErrors
    )
    {
        foreach (var group in targets.GroupBy(
                         static target => target.MessageType.AssemblyQualifiedName!,
                         StringComparer.Ordinal
                     )
                    .OrderBy(static group => group.Key, StringComparer.Ordinal))
        {
            var unnamedTargetCount = group.Count(static target => string.IsNullOrWhiteSpace(target.TargetName));
            var messageType = group.First().MessageType;
            var messageTypeName = messageType.FullName ?? messageType.Name;

            if (unnamedTargetCount > 1)
            {
                validationErrors.Add(
                    $"Message '{messageTypeName}' configures multiple default RabbitMQ outbound targets."
                );
            }

            foreach (var target in group
                        .OrderBy(static target => target.TargetName ?? string.Empty, StringComparer.Ordinal))
            {
                ValidateTarget(
                    target,
                    exchangesByName,
                    channelGroupsByName,
                    defaultPublisherConfirmMode,
                    validationErrors
                );
            }
        }
    }

    private void ValidateTarget(
        RabbitMqOutboundTargetDefinition target,
        IReadOnlyDictionary<string, RabbitMqExchangeDefinition> exchangesByName,
        IReadOnlyDictionary<string, RabbitMqOutboundChannelGroupDefinition> channelGroupsByName,
        RabbitMqPublisherConfirmMode defaultPublisherConfirmMode,
        ICollection<string> validationErrors
    )
    {
        var targetDescription = GetTargetDescription(target);

        if (!exchangesByName.TryGetValue(target.ExchangeName, out var exchange))
        {
            validationErrors.Add($"{targetDescription} references unknown exchange '{target.ExchangeName}'.");
        }
        else
        {
            ValidateTargetAgainstExchange(target, exchange, targetDescription, validationErrors);
        }

        if (!string.IsNullOrWhiteSpace(target.ChannelGroupName) &&
            !channelGroupsByName.ContainsKey(target.ChannelGroupName!))
        {
            validationErrors.Add(
                $"{targetDescription} references unknown channel group '{target.ChannelGroupName}'."
            );
        }

        if (target.IsMandatory &&
            TryGetPublisherConfirmMode(
                target,
                channelGroupsByName,
                defaultPublisherConfirmMode,
                out var publisherConfirmMode
            ) &&
            publisherConfirmMode == RabbitMqPublisherConfirmMode.FireAndForget)
        {
            validationErrors.Add(
                $"{targetDescription} enables mandatory routing but its effective channel group uses fire-and-forget publishing."
            );
        }

        if (target.SerializerType is null)
        {
            validationErrors.Add($"{targetDescription} must configure a serializer.");
        }
        else if (!typeof(IMessageSerializer).IsAssignableFrom(target.SerializerType))
        {
            validationErrors.Add(
                $"Serializer '{target.SerializerType}' for {targetDescription.ToLowerInvariant()} does not implement '{typeof(IMessageSerializer)}'."
            );
        }
        else if (_resolveSerializer(target.SerializerType) is null)
        {
            validationErrors.Add(
                $"Serializer '{target.SerializerType}' for {targetDescription.ToLowerInvariant()} is not registered in the service provider."
            );
        }

        if (target is RabbitMqRoutingKeyOutboundTargetDefinition routingKeyTarget)
        {
            ValidateRoutingKeyConfiguration(routingKeyTarget, targetDescription, validationErrors);
        }
    }

    private static bool TryGetPublisherConfirmMode(
        RabbitMqOutboundTargetDefinition target,
        IReadOnlyDictionary<string, RabbitMqOutboundChannelGroupDefinition> channelGroupsByName,
        RabbitMqPublisherConfirmMode defaultPublisherConfirmMode,
        out RabbitMqPublisherConfirmMode publisherConfirmMode
    )
    {
        if (string.IsNullOrWhiteSpace(target.ChannelGroupName))
        {
            publisherConfirmMode = defaultPublisherConfirmMode;
            return true;
        }

        if (channelGroupsByName.TryGetValue(target.ChannelGroupName!, out var channelGroup))
        {
            publisherConfirmMode = channelGroup.PublisherConfirmMode ?? defaultPublisherConfirmMode;
            return true;
        }

        publisherConfirmMode = default;
        return false;
    }

    private static void ValidateTargetAgainstExchange(
        RabbitMqOutboundTargetDefinition target,
        RabbitMqExchangeDefinition exchange,
        string targetDescription,
        ICollection<string> validationErrors
    )
    {
        var expectedExchangeType = target switch
        {
            RabbitMqFanoutOutboundTargetDefinition => ExchangeType.Fanout,
            RabbitMqDirectOutboundTargetDefinition => ExchangeType.Direct,
            RabbitMqTopicOutboundTargetDefinition => ExchangeType.Topic,
            RabbitMqHeadersOutboundTargetDefinition => ExchangeType.Headers,
            _ => string.Empty
        };

        if (!string.Equals(exchange.Type, expectedExchangeType, StringComparison.Ordinal))
        {
            validationErrors.Add(
                $"{targetDescription} targets exchange '{exchange.Name}' of type '{exchange.Type}', but requires '{expectedExchangeType}'."
            );
        }
    }

    private static void ValidateRoutingKeyConfiguration(
        RabbitMqRoutingKeyOutboundTargetDefinition target,
        string targetDescription,
        ICollection<string> validationErrors
    )
    {
        var hasRoutingKey = target.RoutingKey is not null;
        var hasRoutingKeyFactory = target.RoutingKeyFactory is not null;

        if (hasRoutingKey == hasRoutingKeyFactory)
        {
            validationErrors.Add(
                $"{targetDescription} must configure either a constant routing key or a routing-key factory."
            );
        }
    }

    private static void ValidateMessageContracts(
        IMessageContractRegistry registry,
        IReadOnlyList<RabbitMqOutboundTargetDefinition> targets,
        ICollection<string> validationErrors
    )
    {
        OutboundTargetContractValidator.CollectValidationErrors(
            registry,
            targets.Select(
                static target => new KeyValuePair<string, Type>(GetTargetName(target), target.MessageType)
            ),
            validationErrors
        );
    }

    private static void ValidateMessageContractDialect(
        MessageContractRegistry? dialect,
        IReadOnlyList<RabbitMqOutboundTargetDefinition> targets,
        ICollection<string> validationErrors
    )
    {
        if (dialect is null)
        {
            return;
        }

        var targetMessageTypes = targets.Select(static target => target.MessageType).ToArray();

        foreach (var messageType in dialect.RegisteredMessageTypes)
        {
            if (targetMessageTypes.Any(targetMessageType => targetMessageType.IsAssignableFrom(messageType)))
            {
                continue;
            }

            validationErrors.Add(
                $"RabbitMQ outbound message-contract dialect maps message type '{messageType}', but no outbound target publishes that type on this topology."
            );
        }
    }

    private static void ValidateInboundChannelGroupDefinitions(
        IReadOnlyList<RabbitMqInboundChannelGroupDefinition> channelGroups,
        ICollection<string> validationErrors
    )
    {
        foreach (var channelGroup in channelGroups.OrderBy(static group => group.Name, StringComparer.Ordinal))
        {
            if (channelGroup.MaximumChannelCount < 1)
            {
                validationErrors.Add(
                    $"Channel group '{channelGroup.Name}' maximum channel count must be greater than zero."
                );
            }

            if (channelGroup.PrefetchCount == 0)
            {
                validationErrors.Add(
                    $"Channel group '{channelGroup.Name}' prefetch count must be greater than zero."
                );
            }

            if (channelGroup.ConsumerDispatchConcurrency == 0)
            {
                validationErrors.Add(
                    $"Channel group '{channelGroup.Name}' consumer dispatch concurrency must be greater than zero."
                );
            }

            if (channelGroup.Name.StartsWith(
                    RabbitMqInboundChannelGroupDefinition.ReservedImplicitNamePrefix,
                    StringComparison.Ordinal
                ))
            {
                validationErrors.Add(
                    $"Channel group '{channelGroup.Name}' uses reserved name prefix '{RabbitMqInboundChannelGroupDefinition.ReservedImplicitNamePrefix}'."
                );
            }
        }
    }

    private static void ValidateInboundChannelGroupUsage(
        IReadOnlyList<RabbitMqInboundChannelGroupDefinition> channelGroups,
        IReadOnlyList<RabbitMqInboundConsumerDefinition> consumers,
        ICollection<string> validationErrors
    )
    {
        var referencedChannelGroups = new HashSet<string>(
            consumers
               .Where(static consumer => !string.IsNullOrWhiteSpace(consumer.ChannelGroupName))
               .Select(static consumer => consumer.ChannelGroupName!),
            StringComparer.Ordinal
        );

        foreach (var channelGroupName in channelGroups
                    .Select(static group => group.Name)
                    .Distinct(StringComparer.Ordinal)
                    .OrderBy(static name => name, StringComparer.Ordinal))
        {
            if (!referencedChannelGroups.Contains(channelGroupName))
            {
                validationErrors.Add(
                    $"Channel group '{channelGroupName}' is configured but no inbound endpoint references it."
                );
            }
        }
    }

    private void ValidateConsumers(
        IReadOnlyList<RabbitMqInboundConsumerDefinition> consumers,
        IReadOnlyDictionary<string, RabbitMqQueueDefinition> queuesByName,
        IReadOnlyDictionary<string, RabbitMqInboundChannelGroupDefinition> channelGroupsByName,
        IMessageContractRegistry effectiveMessageContracts,
        ICollection<string> validationErrors
    )
    {
        Dictionary<string, RabbitMqInboundHandlerDefinition> endpointNames = new (StringComparer.Ordinal);
        HashSet<InboundEndpointSelectionKey> dispatchKeys = new (InboundEndpointSelectionKeyComparer.Instance);
        HashSet<string> queueNames = new (StringComparer.Ordinal);

        foreach (var consumer in OrderConsumers(consumers))
        {
            var queueType = ResolveConsumerQueueType(consumer, queuesByName, validationErrors);

            if (!queueNames.Add(consumer.QueueName))
            {
                validationErrors.Add(
                    $"Queue '{consumer.QueueName}' is configured by multiple Consume(...) calls."
                );
            }

            if (consumer.Handlers.Length == 0)
            {
                validationErrors.Add($"Consume('{consumer.QueueName}') declares no handlers.");
            }

            if (!queuesByName.ContainsKey(consumer.QueueName))
            {
                validationErrors.Add(
                    $"Inbound consumer references unknown queue '{consumer.QueueName}'."
                );
            }
            else if (queuesByName[consumer.QueueName].DeclareMode == RabbitMqDeclareMode.Delete)
            {
                validationErrors.Add(
                    $"Inbound consumer for queue '{consumer.QueueName}' references a queue declared with Delete mode; remove the Consume(…) call or change the queue's declare mode."
                );
            }

            if (!string.IsNullOrWhiteSpace(consumer.ChannelGroupName) &&
                !channelGroupsByName.ContainsKey(consumer.ChannelGroupName!))
            {
                validationErrors.Add(
                    $"Inbound consumer for queue '{consumer.QueueName}' references unknown channel group '{consumer.ChannelGroupName}'."
                );
            }

            if (HasExplicitRedeliveryClassifier(consumer) && queueType != RabbitMqQueueType.Quorum)
            {
                validationErrors.Add(
                    $"Inbound consumer for queue '{consumer.QueueName}' configures redelivery, but the effective queue type is '{FormatQueueType(queueType)}'. Redelivery classifiers require a quorum queue with a broker delivery limit; call AsQuorumQueue() on the queue declaration or QueueType(RabbitMqQueueType.Quorum) on the consumer."
                );
            }

            // Queue-type-incompatible knob guards for queues that a consumer references. The effective queue type
            // is resolved through the consumer (which accounts for an explicit QueueType(Quorum) on a passive
            // queue), so the guards are correct even when the queue definition itself does not carry an
            // x-queue-type argument. Queues without consumers are guarded in ValidateQueueDefinitions.
            if (queuesByName.TryGetValue(consumer.QueueName, out var consumerQueue) &&
                consumerQueue.DeclareMode != RabbitMqDeclareMode.Delete)
            {
                ValidateQueueKnobs(consumerQueue, queueType, validationErrors);
            }

            ValidateInspectorChain(consumer, validationErrors);

            var orderedHandlers = OrderHandlers(consumer.Handlers).ToArray();
            var recognizers = ResolveRecognizers(consumer, effectiveMessageContracts, validationErrors);
            var recognizerMappings = MapRecognizersToHandlers(
                consumer.QueueName,
                orderedHandlers,
                recognizers,
                validationErrors
            );

            foreach (var handler in orderedHandlers)
            {
                ValidateServiceRegistrations(handler, validationErrors);
                ValidateAckMode(handler, validationErrors);

                var hasContract = TryGetMessageContractDiscriminators(
                    effectiveMessageContracts,
                    handler.MessageType,
                    out var canonicalDiscriminator,
                    out var inboundDiscriminators
                );
                var recognizerDiscriminators = recognizerMappings
                   .Where(mapping => ReferenceEquals(mapping.Handler, handler))
                   .Select(static mapping => mapping.Recognizer.Discriminator)
                   .ToArray();

                if (!hasContract && recognizerDiscriminators.Length == 0)
                {
                    validationErrors.Add(
                        $"Inbound endpoint for message '{GetTypeName(handler.MessageType)}' consumes unregistered CloudEvents message type. Register its canonical discriminator with MessageContractRegistryBuilder.Map<T>(...)."
                    );
                    continue;
                }

                if (inboundDiscriminators.Count == 0 && recognizerDiscriminators.Length == 0)
                {
                    validationErrors.Add(
                        $"Inbound endpoint for message '{GetTypeName(handler.MessageType)}' has no inbound CloudEvents discriminators. Use MessageContractRegistryBuilder.Map<T>(...) instead of MapOutbound<T>(...)."
                    );
                    continue;
                }

                var primaryDiscriminator = hasContract && inboundDiscriminators.Count > 0 ?
                    canonicalDiscriminator! :
                    recognizerDiscriminators[0];
                var endpointName = handler.EndpointName ?? $"{consumer.QueueName}:{primaryDiscriminator}";

                if (!endpointNames.TryAdd(endpointName, handler))
                {
                    validationErrors.Add($"Inbound endpoint name '{endpointName}' is configured multiple times.");
                }

                foreach (var discriminator in inboundDiscriminators
                            .Concat(recognizerDiscriminators)
                            .Distinct(StringComparer.Ordinal))
                {
                    var dispatchKey = new InboundEndpointSelectionKey(consumer.QueueName, discriminator);

                    if (!dispatchKeys.Add(dispatchKey))
                    {
                        validationErrors.Add(
                            $"Inbound endpoint discriminator '{discriminator}' is configured multiple times for queue '{consumer.QueueName}'."
                        );
                    }
                }
            }
        }
    }

    private static bool HasExplicitRedeliveryClassifier(RabbitMqInboundConsumerDefinition consumer)
    {
        return consumer.RedeliveryClassifier is not null ||
               consumer.Handlers.Any(static handler => handler.RedeliveryClassifier is not null);
    }

    private static RedeliveryClassifier GetDefaultRedeliveryClassifier(RabbitMqQueueType queueType)
    {
        return queueType == RabbitMqQueueType.Quorum ?
            RedeliveryClassifier.RetryUnlessPoison :
            RedeliveryClassifier.RejectAll;
    }

    private static RabbitMqQueueType ResolveConsumerQueueType(
        RabbitMqInboundConsumerDefinition consumer,
        IReadOnlyDictionary<string, RabbitMqQueueDefinition> queuesByName,
        ICollection<string>? validationErrors
    )
    {
        var explicitQueueType = consumer.QueueType;

        if (explicitQueueType is not null &&
            !Enum.IsDefined(typeof(RabbitMqQueueType), explicitQueueType.Value))
        {
            validationErrors?.Add(
                $"Inbound consumer for queue '{consumer.QueueName}' uses unsupported queue type '{explicitQueueType}'."
            );
            explicitQueueType = RabbitMqQueueType.Unknown;
        }

        if (!queuesByName.TryGetValue(consumer.QueueName, out var queue))
        {
            return explicitQueueType ?? RabbitMqQueueType.Unknown;
        }

        var declaredQueueType = queue.DeclareMode == RabbitMqDeclareMode.Active ?
            GetDeclaredQueueType(queue) :
            RabbitMqQueueType.Unknown;

        if (explicitQueueType is null)
        {
            return declaredQueueType;
        }

        if (queue.DeclareMode == RabbitMqDeclareMode.Active &&
            TryGetQueueTypeArgument(queue, out var declaredQueueTypeValue) &&
            declaredQueueType != explicitQueueType.Value)
        {
            validationErrors?.Add(
                $"Inbound consumer for queue '{consumer.QueueName}' asserts queue type '{FormatQueueType(explicitQueueType.Value)}', but active queue declaration '{queue.Name}' sets x-queue-type '{declaredQueueTypeValue}'."
            );
        }

        return explicitQueueType.Value;
    }

    private void ValidateInspectorChain(
        RabbitMqInboundConsumerDefinition consumer,
        ICollection<string> validationErrors
    )
    {
        if (consumer.InspectorChain.Length == 0)
        {
            validationErrors.Add(
                $"Inbound inspector chain for queue '{consumer.QueueName}' must contain at least one entry."
            );
            return;
        }

        foreach (var entry in consumer.InspectorChain)
        {
            switch (entry)
            {
                case ServiceInboundMessageInspectorChainEntry serviceEntry:
                    if (!typeof(IInboundMessageInspector).IsAssignableFrom(serviceEntry.InspectorType))
                    {
                        validationErrors.Add(
                            $"Inbound inspector '{serviceEntry.InspectorType}' for queue '{consumer.QueueName}' does not implement '{typeof(IInboundMessageInspector)}'."
                        );
                    }
                    else if (!_isServiceRegistered(serviceEntry.InspectorType))
                    {
                        validationErrors.Add(
                            $"Inbound inspector '{serviceEntry.InspectorType}' for queue '{consumer.QueueName}' is not registered."
                        );
                    }

                    break;
                case RecognizerInboundMessageInspectorChainEntry:
                    break;
                case null:
                    validationErrors.Add(
                        $"Inbound inspector chain for queue '{consumer.QueueName}' contains a null entry."
                    );
                    break;
                default:
                    validationErrors.Add(
                        $"Inbound inspector chain entry '{GetTypeName(entry.GetType())}' for queue '{consumer.QueueName}' is not supported."
                    );
                    break;
            }
        }
    }

    private static IReadOnlyList<ResolvedInboundRecognizer> ResolveRecognizers(
        RabbitMqInboundConsumerDefinition consumer,
        IMessageContractRegistry effectiveMessageContracts,
        ICollection<string>? validationErrors
    )
    {
        List<ResolvedInboundRecognizer> recognizers = [];

        foreach (var recognizerEntry in consumer.InspectorChain.OfType<RecognizerInboundMessageInspectorChainEntry>())
        {
            string discriminator;

            try
            {
                discriminator = ResolveRecognizerDiscriminator(recognizerEntry, effectiveMessageContracts);
            }
            catch (MessageContractNotRegisteredException) when (validationErrors is not null)
            {
                validationErrors.Add(
                    $"Inbound recognizer for message '{GetTypeName(recognizerEntry.MessageType)}' on queue '{consumer.QueueName}' uses As<T>() but the type is not registered. Use As<T>(explicitDiscriminator) or register the contract with MessageContractRegistryBuilder.Map<T>(...)."
                );
                continue;
            }

            recognizers.Add(new ResolvedInboundRecognizer(recognizerEntry, discriminator));
        }

        return recognizers;
    }

    private static string ResolveRecognizerDiscriminator(
        RecognizerInboundMessageInspectorChainEntry recognizerEntry,
        IMessageContractRegistry effectiveMessageContracts
    )
    {
        return recognizerEntry.ExplicitDiscriminator ??
               effectiveMessageContracts.GetDiscriminator(recognizerEntry.MessageType);
    }

    private static IReadOnlyList<InboundRecognizerHandlerMapping> MapRecognizersToHandlers(
        string queueName,
        IReadOnlyList<RabbitMqInboundHandlerDefinition> handlers,
        IReadOnlyList<ResolvedInboundRecognizer> recognizers,
        ICollection<string>? validationErrors
    )
    {
        List<InboundRecognizerHandlerMapping> mappings = [];

        foreach (var recognizer in recognizers)
        {
            var matchingHandlers = handlers
               .Where(handler => handler.MessageType == recognizer.Entry.MessageType)
               .ToArray();

            if (matchingHandlers.Length == 0)
            {
                matchingHandlers = handlers
                   .Where(handler => handler.MessageType.IsAssignableFrom(recognizer.Entry.MessageType))
                   .ToArray();
            }

            if (matchingHandlers.Length == 0)
            {
                validationErrors?.Add(
                    $"Inbound recognizer for message '{GetTypeName(recognizer.Entry.MessageType)}' maps discriminator '{recognizer.Discriminator}' on queue '{queueName}', but no handler on that queue handles an assignable message type."
                );
                continue;
            }

            if (matchingHandlers.Length > 1)
            {
                validationErrors?.Add(
                    $"Inbound recognizer for message '{GetTypeName(recognizer.Entry.MessageType)}' maps discriminator '{recognizer.Discriminator}' on queue '{queueName}', but multiple handlers on that queue handle an assignable message type."
                );
                continue;
            }

            mappings.Add(new InboundRecognizerHandlerMapping(recognizer, matchingHandlers[0]));
        }

        return mappings;
    }

    private static bool TryGetMessageContractDiscriminators(
        IMessageContractRegistry effectiveMessageContracts,
        Type messageType,
        out string? canonicalDiscriminator,
        out IReadOnlyCollection<string> inboundDiscriminators
    )
    {
        try
        {
            canonicalDiscriminator = effectiveMessageContracts.GetDiscriminator(messageType);
            inboundDiscriminators = effectiveMessageContracts.GetInboundDiscriminators(messageType);
            return true;
        }
        catch (MessageContractNotRegisteredException)
        {
            canonicalDiscriminator = null;
            inboundDiscriminators = Array.Empty<string>();
            return false;
        }
    }

    private void ValidateServiceRegistrations(
        RabbitMqInboundHandlerDefinition handler,
        ICollection<string> validationErrors
    )
    {
        if (!_isServiceRegistered(handler.HandlerType))
        {
            validationErrors.Add(
                $"Inbound handler '{handler.HandlerType}' for message '{GetTypeName(handler.MessageType)}' is not registered."
            );
        }

        if (!typeof(IMessageDeserializer).IsAssignableFrom(handler.DeserializerType))
        {
            validationErrors.Add(
                $"Inbound deserializer '{handler.DeserializerType}' for message '{GetTypeName(handler.MessageType)}' does not implement '{typeof(IMessageDeserializer)}'."
            );
        }
        else if (!_isServiceRegistered(handler.DeserializerType))
        {
            validationErrors.Add(
                $"Inbound deserializer '{handler.DeserializerType}' for message '{GetTypeName(handler.MessageType)}' is not registered."
            );
        }
    }

    private void ValidatePipeline(
        RabbitMqTopologyConfiguration configuration,
        ICollection<string> validationErrors
    )
    {
        if (configuration.Consumers.Count == 0)
        {
            return;
        }

        if (!typeof(IMessageMiddleware).IsAssignableFrom(configuration.DeserializationMiddlewareType))
        {
            validationErrors.Add(
                $"Inbound deserialization middleware '{configuration.DeserializationMiddlewareType}' must implement '{typeof(IMessageMiddleware)}'."
            );
            return;
        }

        if (!_isServiceRegistered(configuration.DeserializationMiddlewareType))
        {
            validationErrors.Add(
                $"Inbound deserialization middleware '{configuration.DeserializationMiddlewareType}' is not registered."
            );
        }
    }

    private static void ValidateAckMode(
        RabbitMqInboundHandlerDefinition handler,
        ICollection<string> validationErrors
    )
    {
        if (!Enum.IsDefined(typeof(MessageAckMode), handler.AckMode))
        {
            validationErrors.Add(
                $"Inbound endpoint for message '{GetTypeName(handler.MessageType)}' uses unsupported acknowledgement mode '{handler.AckMode}'."
            );
        }
    }

    private static void ValidateBindings(
        IReadOnlyList<RabbitMqBindingDefinition> bindings,
        IReadOnlyDictionary<string, RabbitMqExchangeDefinition> exchangesByName,
        IReadOnlyDictionary<string, RabbitMqQueueDefinition> queuesByName,
        ICollection<string> validationErrors
    )
    {
        foreach (var binding in bindings.OrderBy(static binding => binding.SourceExchangeName, StringComparer.Ordinal)
                    .ThenBy(static binding => GetBindingDestinationName(binding), StringComparer.Ordinal)
                    .ThenBy(static binding => binding.RoutingKey, StringComparer.Ordinal))
        {
            if (!Enum.IsDefined(typeof(RabbitMqBindingMode), binding.BindingMode))
            {
                validationErrors.Add(
                    $"{GetBindingDescription(binding)} uses unsupported binding mode '{binding.BindingMode}'."
                );
            }

            if (!exchangesByName.ContainsKey(binding.SourceExchangeName))
            {
                validationErrors.Add(
                    $"{GetBindingDescription(binding)} references unknown source exchange '{binding.SourceExchangeName}'."
                );
            }

            switch (binding)
            {
                case RabbitMqQueueBindingDefinition queueBinding when !queuesByName.ContainsKey(queueBinding.QueueName):
                    validationErrors.Add(
                        $"{GetBindingDescription(queueBinding)} references unknown queue '{queueBinding.QueueName}'."
                    );
                    break;

                case RabbitMqExchangeBindingDefinition exchangeBinding
                    when !exchangesByName.ContainsKey(exchangeBinding.DestinationExchangeName):
                    validationErrors.Add(
                        $"{GetBindingDescription(exchangeBinding)} references unknown destination exchange '{exchangeBinding.DestinationExchangeName}'."
                    );
                    break;
            }
        }
    }

    private static RabbitMqQueueType GetDeclaredQueueType(RabbitMqQueueDefinition queue)
    {
        if (!TryGetQueueTypeArgument(queue, out var queueType))
        {
            return RabbitMqQueueType.Unknown;
        }

        return queueType.ToLowerInvariant() switch
        {
            "classic" => RabbitMqQueueType.Classic,
            "quorum" => RabbitMqQueueType.Quorum,
            _ => RabbitMqQueueType.Unknown
        };
    }

    private static bool TryGetQueueTypeArgument(RabbitMqQueueDefinition queue, out string queueType)
    {
        if (queue.Arguments.TryGetValue("x-queue-type", out var rawQueueType) &&
            rawQueueType is string value &&
            !string.IsNullOrWhiteSpace(value))
        {
            queueType = value;
            return true;
        }

        queueType = string.Empty;
        return false;
    }

    private static string FormatQueueType(RabbitMqQueueType queueType)
    {
        return queueType switch
        {
            RabbitMqQueueType.Classic => "classic",
            RabbitMqQueueType.Quorum => "quorum",
            RabbitMqQueueType.Unknown => "unknown",
            _ => queueType.ToString()
        };
    }

    private static Dictionary<string, T> ToDictionary<T>(IEnumerable<T> values, Func<T, string> getName)
    {
        return values
           .GroupBy(getName, StringComparer.Ordinal)
           .Select(static group => group.First())
           .ToDictionary(getName, StringComparer.Ordinal);
    }

    private static IEnumerable<string> FindDuplicateNames(IEnumerable<string> names, string entityDescription)
    {
        return names
           .GroupBy(static name => name, StringComparer.Ordinal)
           .Where(static group => group.Count() > 1)
           .Select(group => $"Duplicate {entityDescription} '{group.Key}' is configured.");
    }

    private static string GetTargetName(RabbitMqOutboundTargetDefinition target)
    {
        return string.IsNullOrWhiteSpace(target.TargetName) ?
            target.MessageType.FullName ?? target.MessageType.Name :
            target.TargetName!;
    }

    private static string GetTargetDescription(RabbitMqOutboundTargetDefinition target)
    {
        var messageTypeName = target.MessageType.FullName ?? target.MessageType.Name;

        return string.IsNullOrWhiteSpace(target.TargetName) ?
            $"Outbound target for message '{messageTypeName}'" :
            $"Outbound target for message '{messageTypeName}' and target '{target.TargetName}'";
    }

    private static string GetBindingDescription(RabbitMqBindingDefinition binding)
    {
        return binding switch
        {
            RabbitMqQueueBindingDefinition queueBinding =>
                $"Queue binding from exchange '{queueBinding.SourceExchangeName}' to queue '{queueBinding.QueueName}'",
            RabbitMqExchangeBindingDefinition exchangeBinding =>
                $"Exchange binding from exchange '{exchangeBinding.SourceExchangeName}' to exchange '{exchangeBinding.DestinationExchangeName}'",
            _ => "RabbitMQ binding"
        };
    }

    private static string GetBindingDestinationName(RabbitMqBindingDefinition binding)
    {
        return binding switch
        {
            RabbitMqQueueBindingDefinition queueBinding => queueBinding.QueueName,
            RabbitMqExchangeBindingDefinition exchangeBinding => exchangeBinding.DestinationExchangeName,
            _ => string.Empty
        };
    }

    private static string GetTypeName(Type messageType)
    {
        return messageType.FullName ?? messageType.Name;
    }

    private void LogWorstCaseChannelCount(int worstCaseChannelCount, string description)
    {
        var logger = _loggerFactory.CreateLogger(typeof(RabbitMqTopologyCompiler));
        logger.LogInformation(
            "RabbitMQ topology may open up to {ChannelCount} channels ({Description})",
            worstCaseChannelCount,
            description
        );
    }

    private void WarnWhenEmpty(RabbitMqTopology topology)
    {
        if (!topology.IsEmpty)
        {
            return;
        }

        _loggerFactory.CreateLogger(typeof(RabbitMqTopologyCompiler)).LogWarning(
            "RabbitMQ topology '{TopologyName}' is empty: it declares no outbound targets and no inbound endpoints",
            topology.Name
        );
    }

    private sealed class InboundEndpointPlan
    {
        public InboundEndpointPlan(
            RabbitMqInboundHandlerDefinition handler,
            string primaryDiscriminator,
            IReadOnlyList<string> dispatchDiscriminators,
            RedeliveryClassifier redeliveryClassifier
        )
        {
            Handler = handler;
            PrimaryDiscriminator = primaryDiscriminator;
            DispatchDiscriminators = dispatchDiscriminators;
            RedeliveryClassifier = redeliveryClassifier;
        }

        public RabbitMqInboundHandlerDefinition Handler { get; }

        public string PrimaryDiscriminator { get; }

        public IReadOnlyList<string> DispatchDiscriminators { get; }

        public RedeliveryClassifier RedeliveryClassifier { get; }
    }

    private sealed class ResolvedInboundRecognizer
    {
        public ResolvedInboundRecognizer(
            RecognizerInboundMessageInspectorChainEntry entry,
            string discriminator
        )
        {
            Entry = entry;
            Discriminator = discriminator;
        }

        public RecognizerInboundMessageInspectorChainEntry Entry { get; }

        public string Discriminator { get; }
    }

    private sealed class InboundRecognizerHandlerMapping
    {
        public InboundRecognizerHandlerMapping(
            ResolvedInboundRecognizer recognizer,
            RabbitMqInboundHandlerDefinition handler
        )
        {
            Recognizer = recognizer;
            Handler = handler;
        }

        public ResolvedInboundRecognizer Recognizer { get; }

        public RabbitMqInboundHandlerDefinition Handler { get; }
    }
}
