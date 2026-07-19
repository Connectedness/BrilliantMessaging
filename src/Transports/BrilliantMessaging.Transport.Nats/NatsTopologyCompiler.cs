using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;
using BrilliantMessaging.Core.Messaging.Outbound;
using BrilliantMessaging.Transport.Nats.Inbound;
using BrilliantMessaging.Transport.Nats.Outbound;
using Microsoft.Extensions.DependencyInjection;

namespace BrilliantMessaging.Transport.Nats;

/// <summary>
/// Validates and compiles NATS JetStream topology configuration.
/// </summary>
public sealed class NatsTopologyCompiler
{
    private static readonly MethodInfo CreateEndpointMethod = typeof(NatsTopologyCompiler)
       .GetMethod(nameof(CreateEndpointCore), BindingFlags.Static | BindingFlags.NonPublic)!;

    private static readonly MethodInfo CreateTargetMethod = typeof(NatsTopologyCompiler)
       .GetMethod(nameof(CreateTargetCore), BindingFlags.Static | BindingFlags.NonPublic)!;

    private readonly IMessageSerializer _defaultSerializer;
    private readonly IMessageContractRegistry _globalMessageContractRegistry;
    private readonly Func<Type, IMessageSerializer?> _resolveSerializer;
    private readonly Func<Type, bool> _serviceIsRegistered;

    /// <summary>
    /// Initializes a new instance of the <see cref="NatsTopologyCompiler" /> class.
    /// </summary>
    public NatsTopologyCompiler(
        IMessageContractRegistry globalMessageContractRegistry,
        IMessageSerializer defaultSerializer,
        Func<Type, IMessageSerializer?> resolveSerializer,
        Func<Type, bool> serviceIsRegistered
    )
    {
        _globalMessageContractRegistry = globalMessageContractRegistry ??
                                         throw new ArgumentNullException(nameof(globalMessageContractRegistry));
        _defaultSerializer = defaultSerializer ?? throw new ArgumentNullException(nameof(defaultSerializer));
        _resolveSerializer = resolveSerializer ?? throw new ArgumentNullException(nameof(resolveSerializer));
        _serviceIsRegistered = serviceIsRegistered ?? throw new ArgumentNullException(nameof(serviceIsRegistered));
    }

    /// <summary>
    /// Compiles a topology.
    /// </summary>
    public NatsTopology Compile(
        string topologyName,
        NatsTopologyConfiguration configuration,
        NatsConnectionProvider connectionProvider
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

        var effectiveRegistry = configuration.MessageContractDialect is null ?
            _globalMessageContractRegistry :
            new EffectiveMessageContractRegistry(_globalMessageContractRegistry, configuration.MessageContractDialect);
        var errors = Validate(configuration, effectiveRegistry);
        if (errors.Count > 0)
        {
            throw new TopologyValidationException(errors);
        }

        var pipeline = BuildPipeline(configuration);
        Dictionary<string, InboundEndpoint> endpointsByName = new (StringComparer.Ordinal);
        var consumers =
            CompileConsumers(topologyName, configuration, effectiveRegistry, endpointsByName);
        var (targets, defaultTargets, namedTargets) = CompileTargets(
            topologyName,
            configuration,
            effectiveRegistry,
            connectionProvider
        );

        var data = TopologyData.PrepareTopologyDataStructures(defaultTargets, namedTargets, endpointsByName);
        return new NatsTopology(
            topologyName,
            data,
            effectiveRegistry,
            configuration.Streams,
            targets,
            consumers,
            consumers.SelectMany(static consumer => consumer.EndpointsByDiscriminator.Values).Distinct().ToList(),
            pipeline,
            configuration.ShutdownTimeout,
            configuration.ProvisioningMode,
            configuration.AckProgressEnabled,
            connectionProvider
        );
    }

    private List<NatsInboundConsumer> CompileConsumers(
        string topologyName,
        NatsTopologyConfiguration configuration,
        IMessageContractRegistry messageContractRegistry,
        IDictionary<string, InboundEndpoint> endpointsByName
    )
    {
        List<NatsInboundConsumer> consumers = [];
        foreach (var consumer in configuration.Consumers)
        {
            Dictionary<string, NatsInboundEndpoint> endpointsByDiscriminator = new (StringComparer.Ordinal);
            var source = consumer.FilterSubject ?? consumer.DurableName;
            foreach (var handler in consumer.Handlers)
            {
                var discriminator = messageContractRegistry.GetDiscriminator(handler.MessageType);
                var endpointName = handler.EndpointName ?? $"{consumer.DurableName}:{discriminator}";
                var endpoint = CreateEndpoint(
                    handler,
                    consumer.RedeliveryClassifier,
                    topologyName,
                    source,
                    endpointName,
                    discriminator
                );
                endpointsByDiscriminator.Add(discriminator, endpoint);
                endpointsByName.Add(endpoint.Name, endpoint);
            }

            consumers.Add(
                new NatsInboundConsumer(
                    consumer.StreamName,
                    consumer.DurableName,
                    consumer.FilterSubject,
                    consumer.Concurrency,
                    consumer.AckWait,
                    consumer.MaxDeliver,
                    consumer.MaxAckPending,
                    consumer.MaxBufferedMessages,
                    consumer.DeadLetterSubject,
                    endpointsByDiscriminator
                )
            );
        }

        return consumers;
    }

    private (IReadOnlyList<OutboundTarget> Targets, Dictionary<Type, OutboundTarget> DefaultTargets,
        Dictionary<string, OutboundTarget> NamedTargets)
        CompileTargets(
            string topologyName,
            NatsTopologyConfiguration configuration,
            IMessageContractRegistry messageContractRegistry,
            NatsConnectionProvider connectionProvider
        )
    {
        List<OutboundTarget> targets = [];
        Dictionary<Type, OutboundTarget> defaultTargets = new ();
        Dictionary<string, OutboundTarget> namedTargets = new (StringComparer.Ordinal);

        foreach (var definition in configuration.Targets)
        {
            var serializer = definition.SerializerType is null ?
                _defaultSerializer :
                _resolveSerializer(definition.SerializerType) ??
                throw new InvalidOperationException($"Serializer '{definition.SerializerType}' is not registered.");
            var targetName = definition.TargetName ?? $"{definition.Subject}:{definition.MessageType.FullName}";
            var target = CreateTarget(
                definition,
                targetName,
                topologyName,
                serializer,
                messageContractRegistry,
                connectionProvider
            );
            targets.Add(target);

            if (definition.TargetName is null)
            {
                defaultTargets.Add(definition.MessageType, target);
            }
            else
            {
                namedTargets.Add(definition.TargetName, target);
            }
        }

        return (targets, defaultTargets, namedTargets);
    }

    private NatsInboundEndpoint CreateEndpoint(
        NatsInboundHandlerDefinition handler,
        RedeliveryClassifier? consumerRedeliveryClassifier,
        string topologyName,
        string source,
        string endpointName,
        string discriminator
    )
    {
        var closedMethod = CreateEndpointMethod.MakeGenericMethod(handler.MessageType);
        return (NatsInboundEndpoint) closedMethod.Invoke(
            null,
            [handler, consumerRedeliveryClassifier, topologyName, source, endpointName, discriminator]
        )!;
    }

    private static NatsInboundEndpoint CreateEndpointCore<TMessage>(
        NatsInboundHandlerDefinition handler,
        RedeliveryClassifier? consumerRedeliveryClassifier,
        string topologyName,
        string source,
        string endpointName,
        string discriminator
    )
    {
        return new NatsInboundEndpoint<TMessage>(
            endpointName,
            topologyName,
            source,
            handler.HandlerType,
            handler.DeserializerType,
            discriminator,
            handler.HandlerInvocation,
            handler.AckMode,
            handler.RedeliveryClassifier ?? consumerRedeliveryClassifier ?? RedeliveryClassifier.RetryUnlessPoison
        );
    }

    private static OutboundTarget CreateTarget(
        NatsOutboundTargetDefinition definition,
        string targetName,
        string topologyName,
        IMessageSerializer serializer,
        IMessageContractRegistry messageContractRegistry,
        NatsConnectionProvider connectionProvider
    )
    {
        var closedMethod = CreateTargetMethod.MakeGenericMethod(definition.MessageType);
        return (OutboundTarget) closedMethod.Invoke(
            null,
            [definition, targetName, topologyName, serializer, messageContractRegistry, connectionProvider]
        )!;
    }

    private static OutboundTarget CreateTargetCore<TMessage>(
        NatsOutboundTargetDefinition definition,
        string targetName,
        string topologyName,
        IMessageSerializer serializer,
        IMessageContractRegistry messageContractRegistry,
        NatsConnectionProvider connectionProvider
    )
    {
        return new NatsOutboundTarget<TMessage>(
            targetName,
            serializer,
            messageContractRegistry,
            topologyName,
            definition.Subject,
            definition.MessageIdDeduplication,
            connectionProvider
        );
    }

    private static MessageDelegate BuildPipeline(NatsTopologyConfiguration configuration)
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

    private List<string> Validate(
        NatsTopologyConfiguration configuration,
        IMessageContractRegistry messageContractRegistry
    )
    {
        List<string> errors = [];
        var streamNames = configuration.Streams.Select(static stream => stream.Name).ToHashSet(StringComparer.Ordinal);
        ValidateStreams(configuration.Streams, errors);
        ValidateTargets(configuration.Targets, messageContractRegistry, errors);
        ValidateConsumers(configuration, streamNames, messageContractRegistry, errors);

        if (configuration.CreateOptions is null)
        {
            errors.Add("NATS connection options must be configured.");
        }

        if (!typeof(IMessageMiddleware).IsAssignableFrom(configuration.DeserializationMiddlewareType))
        {
            errors.Add(
                $"Deserialization middleware '{configuration.DeserializationMiddlewareType}' must implement IMessageMiddleware."
            );
        }
        else if (!_serviceIsRegistered(configuration.DeserializationMiddlewareType))
        {
            errors.Add(
                $"Deserialization middleware '{configuration.DeserializationMiddlewareType}' is not registered."
            );
        }

        return errors;
    }

    private static void ValidateStreams(IReadOnlyList<NatsStreamDefinition> streams, ICollection<string> errors)
    {
        foreach (var group in streams.GroupBy(static stream => stream.Name, StringComparer.Ordinal))
        {
            if (group.Count() > 1)
            {
                errors.Add($"NATS stream '{group.Key}' is configured more than once.");
            }
        }

        foreach (var stream in streams)
        {
            if (!IsValidResourceName(stream.Name))
            {
                errors.Add(
                    $"NATS stream '{stream.Name}' has an invalid name. Stream names must not contain whitespace, control characters, '.', '*', '>', '/', or '\\'."
                );
            }

            if (stream.Subjects.Count == 0)
            {
                errors.Add($"NATS stream '{stream.Name}' must declare at least one subject pattern.");
            }

            foreach (var subject in stream.Subjects)
            {
                if (!IsValidSubject(subject, allowWildcards: true))
                {
                    errors.Add($"NATS stream '{stream.Name}' has invalid subject pattern '{subject}'.");
                }
            }

            if (stream.Replicas is < NatsTopologyBuilderDefaults.MinimumStreamReplicas or
                                   > NatsTopologyBuilderDefaults.MaximumStreamReplicas)
            {
                errors.Add(
                    $"NATS stream '{stream.Name}' must configure a replica count between {NatsTopologyBuilderDefaults.MinimumStreamReplicas} and {NatsTopologyBuilderDefaults.MaximumStreamReplicas}."
                );
            }
        }
    }

    private void ValidateTargets(
        IReadOnlyList<NatsOutboundTargetDefinition> targets,
        IMessageContractRegistry messageContractRegistry,
        ICollection<string> errors
    )
    {
        foreach (var group in targets
                    .Where(static target => target.TargetName is not null)
                    .GroupBy(static target => target.TargetName!, StringComparer.Ordinal))
        {
            if (group.Count() > 1)
            {
                errors.Add($"Outbound target name '{group.Key}' is configured more than once.");
            }
        }

        foreach (var group in targets
                    .Where(static target => target.TargetName is null)
                    .GroupBy(static target => target.MessageType))
        {
            if (group.Count() > 1)
            {
                errors.Add($"Message '{group.Key.FullName}' configures multiple default NATS outbound targets.");
            }
        }

        foreach (var target in targets)
        {
            if (!IsValidSubject(target.Subject, allowWildcards: false))
            {
                errors.Add(
                    $"Outbound target for message '{target.MessageType.FullName}' uses invalid literal subject '{target.Subject}'."
                );
            }

            if (!messageContractRegistry.TryGetDiscriminator(target.MessageType, out _))
            {
                errors.Add(
                    $"Outbound target for message '{target.MessageType.FullName}' has no registered CloudEvents discriminator. Register it with MessageContractRegistryBuilder.Map<T>(...)."
                );
            }

            if (target.SerializerType is not null &&
                !typeof(IMessageSerializer).IsAssignableFrom(target.SerializerType))
            {
                errors.Add(
                    $"Serializer '{target.SerializerType.FullName}' for NATS target '{target.TargetName ?? target.Subject}' must implement IMessageSerializer."
                );
            }
        }
    }

    private void ValidateConsumers(
        NatsTopologyConfiguration configuration,
        HashSet<string> streamNames,
        IMessageContractRegistry messageContractRegistry,
        ICollection<string> errors
    )
    {
        foreach (var group in configuration.Consumers.GroupBy(
                     static consumer => consumer.DurableName,
                     StringComparer.Ordinal
                 ))
        {
            if (group.Count() > 1)
            {
                errors.Add($"NATS durable consumer '{group.Key}' is configured more than once.");
            }
        }

        foreach (var consumer in configuration.Consumers)
        {
            HashSet<Type> handlerMessageTypes = new ();
            if (!IsValidResourceName(consumer.DurableName))
            {
                errors.Add(
                    $"NATS durable consumer '{consumer.DurableName}' has an invalid name. Durable names must not contain whitespace, control characters, '.', '*', '>', '/', or '\\'."
                );
            }

            if (!streamNames.Contains(consumer.StreamName))
            {
                errors.Add(
                    $"NATS consumer '{consumer.DurableName}' references missing stream '{consumer.StreamName}'."
                );
            }

            var filterSubjectIsValid = consumer.FilterSubject is null ||
                                       IsValidSubject(consumer.FilterSubject, allowWildcards: true);
            if (!filterSubjectIsValid)
            {
                errors.Add(
                    $"NATS consumer '{consumer.DurableName}' uses invalid filter subject '{consumer.FilterSubject}'."
                );
            }

            if (consumer.FilterSubject is not null &&
                filterSubjectIsValid &&
                streamNames.Contains(consumer.StreamName) &&
                !configuration.Streams.Any(
                    stream => string.Equals(stream.Name, consumer.StreamName, StringComparison.Ordinal) &&
                              stream.Subjects.Any(
                                  subject => IsValidSubject(subject, allowWildcards: true) &&
                                             SubjectPatternsOverlap(subject, consumer.FilterSubject)
                              )
                ))
            {
                errors.Add(
                    $"NATS consumer '{consumer.DurableName}' filter subject '{consumer.FilterSubject}' does not overlap any subject declared by stream '{consumer.StreamName}'."
                );
            }

            if (consumer.Handlers.Length == 0)
            {
                errors.Add($"NATS consumer '{consumer.DurableName}' must configure at least one handler.");
            }

            foreach (var handler in consumer.Handlers)
            {
                if (!handlerMessageTypes.Add(handler.MessageType))
                {
                    errors.Add(
                        $"NATS consumer '{consumer.DurableName}' configures multiple handlers for message '{handler.MessageType.FullName}'."
                    );
                }

                if (!messageContractRegistry.TryGetDiscriminator(handler.MessageType, out _))
                {
                    errors.Add(
                        $"NATS handler '{handler.HandlerType.FullName}' handles message '{handler.MessageType.FullName}' which has no registered CloudEvents discriminator."
                    );
                }

                if (!_serviceIsRegistered(handler.DeserializerType))
                {
                    errors.Add($"Deserializer '{handler.DeserializerType.FullName}' is not registered.");
                }
            }

            if (consumer.DeadLetterSubject is not null)
            {
                var deadLetterSubjectIsValid = IsValidSubject(
                    consumer.DeadLetterSubject,
                    allowWildcards: false
                );
                if (!deadLetterSubjectIsValid)
                {
                    errors.Add(
                        $"NATS consumer '{consumer.DurableName}' uses invalid dead-letter subject '{consumer.DeadLetterSubject}'."
                    );
                }

                if (deadLetterSubjectIsValid &&
                    !configuration.Streams.Any(stream => Covers(stream.Subjects, consumer.DeadLetterSubject)))
                {
                    errors.Add(
                        $"Dead-letter subject '{consumer.DeadLetterSubject}' for NATS consumer '{consumer.DurableName}' is not covered by a declared stream."
                    );
                }

                if (deadLetterSubjectIsValid &&
                    ConsumerSelectsSubject(configuration.Streams, consumer, consumer.DeadLetterSubject))
                {
                    errors.Add(
                        $"Dead-letter subject '{consumer.DeadLetterSubject}' for NATS consumer '{consumer.DurableName}' is selected by the same consumer. Configure a filter that excludes the dead-letter subject or route it to a different stream."
                    );
                }
            }
        }
    }

    private static bool ConsumerSelectsSubject(
        IReadOnlyList<NatsStreamDefinition> streams,
        NatsInboundConsumerDefinition consumer,
        string subject
    )
    {
        var sourceStreamCoversSubject = streams.Any(
            stream => string.Equals(stream.Name, consumer.StreamName, StringComparison.Ordinal) &&
                      stream.Subjects.Any(
                          pattern => IsValidSubject(pattern, allowWildcards: true) && Covers(pattern, subject)
                      )
        );
        if (!sourceStreamCoversSubject)
        {
            return false;
        }

        return consumer.FilterSubject is null ||
               (IsValidSubject(consumer.FilterSubject, allowWildcards: true) &&
                Covers(consumer.FilterSubject, subject));
    }

    private static bool IsValidSubject(string subject, bool allowWildcards)
    {
        if (string.IsNullOrWhiteSpace(subject) || subject.Any(IsInvalidProtocolCharacter))
        {
            return false;
        }

        var tokens = subject.Split('.');
        for (var i = 0; i < tokens.Length; i++)
        {
            var token = tokens[i];
            if (token.Length == 0)
            {
                return false;
            }

            if (token == ">" && (!allowWildcards || i != tokens.Length - 1))
            {
                return false;
            }

            if (token == "*" && !allowWildcards)
            {
                return false;
            }

            if ((token.Contains('*') || token.Contains('>')) && token is not "*" and not ">")
            {
                return false;
            }
        }

        return true;
    }

    private static bool IsValidResourceName(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            return false;
        }

        foreach (var character in name)
        {
            if (IsInvalidProtocolCharacter(character) || character is '.' or '*' or '>' or '/' or '\\')
            {
                return false;
            }
        }

        return true;
    }

    private static bool IsInvalidProtocolCharacter(char character)
    {
        return char.IsWhiteSpace(character) || char.IsControl(character);
    }

    private static bool Covers(IReadOnlyList<string> patterns, string subject)
    {
        return patterns.Any(pattern => Covers(pattern, subject));
    }

    private static bool Covers(string pattern, string subject)
    {
        var patternTokens = pattern.Split('.');
        var subjectTokens = subject.Split('.');
        for (var i = 0; i < patternTokens.Length; i++)
        {
            if (patternTokens[i] == ">")
            {
                return i < subjectTokens.Length;
            }

            if (i >= subjectTokens.Length)
            {
                return false;
            }

            if (patternTokens[i] == "*")
            {
                continue;
            }

            if (!string.Equals(patternTokens[i], subjectTokens[i], StringComparison.Ordinal))
            {
                return false;
            }
        }

        return patternTokens.Length == subjectTokens.Length;
    }

    private static bool SubjectPatternsOverlap(string leftPattern, string rightPattern)
    {
        var leftTokens = leftPattern.Split('.');
        var rightTokens = rightPattern.Split('.');
        var sharedLength = Math.Min(leftTokens.Length, rightTokens.Length);
        for (var i = 0; i < sharedLength; i++)
        {
            var leftToken = leftTokens[i];
            var rightToken = rightTokens[i];
            if (leftToken == ">" || rightToken == ">")
            {
                return true;
            }

            if (leftToken != "*" &&
                rightToken != "*" &&
                !string.Equals(leftToken, rightToken, StringComparison.Ordinal))
            {
                return false;
            }
        }

        return leftTokens.Length == rightTokens.Length;
    }
}
