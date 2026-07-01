using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;
using BrilliantMessaging.Core.Messaging.Outbound;
using BrilliantMessaging.Transport.InMemory.Inbound;
using BrilliantMessaging.Transport.InMemory.Outbound;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace BrilliantMessaging.Transport.InMemory;

/// <summary>
/// Validates an <see cref="InMemoryTopologyConfiguration" /> and compiles it into a single
/// <see cref="InMemoryTopology" /> backed by an <see cref="InMemoryBroker" />. The compiled topology exposes its
/// outbound targets and inbound endpoints through the Core <see cref="Topology" /> base so the normal publisher,
/// serializers, CloudEvents binding, inbound pipeline, acknowledgement behaviour, and diagnostics stay in the path.
/// </summary>
public sealed class InMemoryTopologyCompiler
{
    private static readonly MethodInfo CreateEndpointMethod = typeof(InMemoryTopologyCompiler)
       .GetMethod(nameof(CreateEndpointCore), BindingFlags.Static | BindingFlags.NonPublic)!;

    private static readonly MethodInfo CreateTargetMethod = typeof(InMemoryTopologyCompiler)
       .GetMethod(nameof(CreateTargetCore), BindingFlags.Static | BindingFlags.NonPublic)!;

    private readonly IMessageSerializer _defaultSerializer;
    private readonly ILoggerFactory _loggerFactory;
    private readonly IMessageContractRegistry _messageContractRegistry;
    private readonly Func<Type, IMessageSerializer?> _resolveSerializer;
    private readonly IInMemoryDelayScheduler _scheduler;
    private readonly IServiceScopeFactory _serviceScopeFactory;

    /// <summary>
    /// Initializes a new instance of the <see cref="InMemoryTopologyCompiler" /> class.
    /// </summary>
    /// <param name="messageContractRegistry">The message-contract registry used to resolve CloudEvents discriminators.</param>
    /// <param name="defaultSerializer">The serializer used for targets that do not configure their own.</param>
    /// <param name="resolveSerializer">A function that resolves a serializer instance for a serializer type.</param>
    /// <param name="serviceScopeFactory">The factory used to create a per-delivery service scope.</param>
    /// <param name="scheduler">The scheduler used to delay retry deliveries.</param>
    /// <param name="loggerFactory">The factory used to create the broker's logger.</param>
    /// <exception cref="ArgumentNullException">Thrown when any argument is <see langword="null" />.</exception>
    public InMemoryTopologyCompiler(
        IMessageContractRegistry messageContractRegistry,
        IMessageSerializer defaultSerializer,
        Func<Type, IMessageSerializer?> resolveSerializer,
        IServiceScopeFactory serviceScopeFactory,
        IInMemoryDelayScheduler scheduler,
        ILoggerFactory loggerFactory
    )
    {
        _messageContractRegistry = messageContractRegistry ??
                                   throw new ArgumentNullException(nameof(messageContractRegistry));
        _defaultSerializer = defaultSerializer ?? throw new ArgumentNullException(nameof(defaultSerializer));
        _resolveSerializer = resolveSerializer ?? throw new ArgumentNullException(nameof(resolveSerializer));
        _serviceScopeFactory = serviceScopeFactory ?? throw new ArgumentNullException(nameof(serviceScopeFactory));
        _scheduler = scheduler ?? throw new ArgumentNullException(nameof(scheduler));
        _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
    }

    /// <summary>
    /// Validates and compiles a topology configuration into a runnable <see cref="InMemoryTopology" />.
    /// </summary>
    /// <param name="topologyName">The name of the topology being compiled.</param>
    /// <param name="configuration">The topology configuration to compile.</param>
    /// <returns>The compiled topology.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="configuration" /> is <see langword="null" />.</exception>
    /// <exception cref="TopologyValidationException">Thrown when the configuration fails validation.</exception>
    public InMemoryTopology Compile(string topologyName, InMemoryTopologyConfiguration configuration)
    {
        if (configuration is null)
        {
            throw new ArgumentNullException(nameof(configuration));
        }

        var validationErrors = Validate(configuration);
        if (validationErrors.Count > 0)
        {
            throw new TopologyValidationException(validationErrors);
        }

        var pipeline = BuildPipeline();

        Dictionary<string, InboundEndpoint> endpointsByName = new (StringComparer.Ordinal);
        var routes = CompileRoutes(topologyName, configuration, endpointsByName);

        var broker = new InMemoryBroker(
            topologyName,
            routes,
            pipeline,
            _serviceScopeFactory,
            _scheduler,
            configuration.ShutdownTimeout,
            configuration.RecordingOptions,
            _loggerFactory.CreateLogger<InMemoryBroker>()
        );

        var (targetsByMessageType, targetsByName) = CompileTargets(topologyName, configuration, broker);

        var data = TopologyData.PrepareTopologyDataStructures(
            targetsByMessageType,
            targetsByName,
            endpointsByName
        );

        return new InMemoryTopology(topologyName, data, broker, routes, configuration.ShutdownTimeout);
    }

    private List<InMemoryConsumerRoute> CompileRoutes(
        string topologyName,
        InMemoryTopologyConfiguration configuration,
        IDictionary<string, InboundEndpoint> endpointsByName
    )
    {
        List<InMemoryConsumerRoute> routes = [];
        var routeIndex = 0;

        foreach (var consumer in configuration.Consumers)
        {
            var classifier = consumer.DeliveryPolicy.Retry is null ?
                RedeliveryClassifier.RejectAll :
                RedeliveryClassifier.RetryUnlessPoison;

            Dictionary<string, InMemoryInboundEndpoint> endpointsByDiscriminator = new (StringComparer.Ordinal);

            foreach (var handler in consumer.Handlers)
            {
                var discriminator = _messageContractRegistry.GetDiscriminator(handler.MessageType);
                var endpointName = handler.EndpointName ?? $"{consumer.Topic}#{routeIndex}:{discriminator}";
                var endpoint = CreateEndpoint(handler, topologyName, endpointName, discriminator, classifier);

                endpointsByDiscriminator.Add(discriminator, endpoint);
                endpointsByName.Add(endpoint.Name, endpoint);
            }

            routes.Add(
                new InMemoryConsumerRoute(
                    consumer.Topic,
                    consumer.Concurrency,
                    consumer.DeliveryPolicy,
                    endpointsByDiscriminator
                )
            );
            routeIndex++;
        }

        return routes;
    }

    private (Dictionary<Type, OutboundTarget> ByMessageType, Dictionary<string, OutboundTarget> ByName) CompileTargets(
        string topologyName,
        InMemoryTopologyConfiguration configuration,
        InMemoryBroker broker
    )
    {
        Dictionary<Type, OutboundTarget> byMessageType = new ();
        Dictionary<string, OutboundTarget> byName = new (StringComparer.Ordinal);

        foreach (var definition in configuration.Targets)
        {
            var serializer = definition.SerializerType is null ?
                _defaultSerializer :
                _resolveSerializer(definition.SerializerType) ??
                throw new InvalidOperationException(
                    $"Serializer '{definition.SerializerType}' is not registered."
                );

            var targetName = definition.TargetName ?? $"{definition.Topic}:{definition.MessageType.FullName}";
            var target = CreateTarget(definition, targetName, topologyName, serializer, broker);

            if (string.IsNullOrWhiteSpace(definition.TargetName))
            {
                byMessageType.Add(definition.MessageType, target);
            }
            else
            {
                byName.Add(definition.TargetName!, target);
            }
        }

        return (byMessageType, byName);
    }

    private static MessageDelegate BuildPipeline()
    {
        MessagePipelineBuilder pipeline = new ();
        pipeline.UseMiddleware<InboundDiagnosticsMiddleware>();
        pipeline.UseMiddleware<FrameworkMessageAcknowledgementMiddleware>();
        pipeline.UseMiddleware<MessageDeserializationMiddleware>();
        return pipeline.Build(static context => context.Endpoint.InvokeHandlerAsync(context));
    }

    private InMemoryInboundEndpoint CreateEndpoint(
        InMemoryInboundHandlerDefinition handler,
        string topologyName,
        string endpointName,
        string discriminator,
        RedeliveryClassifier classifier
    )
    {
        var closedMethod = CreateEndpointMethod.MakeGenericMethod(handler.MessageType);
        return (InMemoryInboundEndpoint) closedMethod.Invoke(
            null,
            [handler, topologyName, endpointName, discriminator, classifier]
        )!;
    }

    private static InMemoryInboundEndpoint CreateEndpointCore<TMessage>(
        InMemoryInboundHandlerDefinition handler,
        string topologyName,
        string endpointName,
        string discriminator,
        RedeliveryClassifier classifier
    )
    {
        return new InMemoryInboundEndpoint<TMessage>(
            endpointName,
            topologyName,
            handler.HandlerType,
            handler.DeserializerType,
            discriminator,
            handler.HandlerInvocation,
            handler.AckMode,
            classifier
        );
    }

    private OutboundTarget CreateTarget(
        InMemoryOutboundTargetDefinition definition,
        string targetName,
        string topologyName,
        IMessageSerializer serializer,
        InMemoryBroker broker
    )
    {
        var closedMethod = CreateTargetMethod.MakeGenericMethod(definition.MessageType);
        return (OutboundTarget) closedMethod.Invoke(
            null,
            [targetName, serializer, _messageContractRegistry, topologyName, definition.Topic, broker]
        )!;
    }

    private static OutboundTarget CreateTargetCore<TMessage>(
        string targetName,
        IMessageSerializer serializer,
        IMessageContractRegistry messageContractRegistry,
        string topologyName,
        string topic,
        InMemoryBroker broker
    )
    {
        return new InMemoryOutboundTarget<TMessage>(
            targetName,
            serializer,
            messageContractRegistry,
            topologyName,
            topic,
            broker
        );
    }

    private List<string> Validate(InMemoryTopologyConfiguration configuration)
    {
        List<string> errors = [];
        var declaredTopics = new HashSet<string>(configuration.Topics, StringComparer.Ordinal);

        ValidateTargets(configuration, declaredTopics, errors);
        ValidateConsumers(configuration, declaredTopics, errors);

        return errors;
    }

    private void ValidateTargets(
        InMemoryTopologyConfiguration configuration,
        HashSet<string> declaredTopics,
        ICollection<string> errors
    )
    {
        foreach (var nameGroup in configuration.Targets
                    .Where(static target => !string.IsNullOrWhiteSpace(target.TargetName))
                    .GroupBy(static target => target.TargetName!, StringComparer.Ordinal))
        {
            if (nameGroup.Count() > 1)
            {
                errors.Add($"Outbound target name '{nameGroup.Key}' is configured more than once.");
            }
        }

        foreach (var messageTypeGroup in configuration.Targets
                    .Where(static target => string.IsNullOrWhiteSpace(target.TargetName))
                    .GroupBy(static target => target.MessageType))
        {
            if (messageTypeGroup.Count() > 1)
            {
                errors.Add(
                    $"Message '{messageTypeGroup.Key.FullName}' configures multiple default in-memory outbound targets."
                );
            }
        }

        foreach (var target in configuration.Targets)
        {
            if (!declaredTopics.Contains(target.Topic))
            {
                errors.Add(
                    $"Outbound target for message '{target.MessageType.FullName}' publishes to undeclared topic '{target.Topic}'. Declare it with Topic(\"{target.Topic}\")."
                );
            }

            if (!_messageContractRegistry.TryGetDiscriminator(target.MessageType, out _))
            {
                errors.Add(
                    $"Outbound target for message '{target.MessageType.FullName}' has no registered CloudEvents discriminator. Register it with MessageContractRegistryBuilder.Map<T>(...)."
                );
            }

            if (target.SerializerType is not null &&
                !typeof(IMessageSerializer).IsAssignableFrom(target.SerializerType))
            {
                errors.Add(
                    $"Serializer '{target.SerializerType}' for the outbound target of message '{target.MessageType.FullName}' does not implement '{typeof(IMessageSerializer)}'."
                );
            }
        }
    }

    private void ValidateConsumers(
        InMemoryTopologyConfiguration configuration,
        HashSet<string> declaredTopics,
        ICollection<string> errors
    )
    {
        foreach (var consumer in configuration.Consumers)
        {
            if (!declaredTopics.Contains(consumer.Topic))
            {
                errors.Add(
                    $"Consumer for topic '{consumer.Topic}' references an undeclared topic. Declare it with Topic(\"{consumer.Topic}\")."
                );
            }

            if (consumer.DeliveryPolicy.DeadLetterTopic is { } deadLetterTopic &&
                !declaredTopics.Contains(deadLetterTopic))
            {
                errors.Add(
                    $"Consumer for topic '{consumer.Topic}' dead-letters to undeclared topic '{deadLetterTopic}'. Declare it with Topic(\"{deadLetterTopic}\")."
                );
            }

            if (consumer.Handlers.Length == 0)
            {
                errors.Add($"Consume('{consumer.Topic}') declares no handlers.");
            }

            HashSet<string> discriminators = new (StringComparer.Ordinal);

            foreach (var handler in consumer.Handlers)
            {
                if (!_messageContractRegistry.TryGetDiscriminator(handler.MessageType, out var discriminator) ||
                    discriminator is null)
                {
                    errors.Add(
                        $"Handler for message '{handler.MessageType.FullName}' on topic '{consumer.Topic}' has no registered CloudEvents discriminator. Register it with MessageContractRegistryBuilder.Map<T>(...)."
                    );
                    continue;
                }

                if (!discriminators.Add(discriminator))
                {
                    errors.Add(
                        $"Consumer for topic '{consumer.Topic}' configures more than one handler for CloudEvents type '{discriminator}'."
                    );
                }
            }
        }
    }
}
