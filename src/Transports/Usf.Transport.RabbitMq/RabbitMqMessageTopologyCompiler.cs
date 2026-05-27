using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Errors;
using Usf.Transport.RabbitMq.Configuration;

namespace Usf.Transport.RabbitMq;

public static class RabbitMqMessageTopologyCompiler
{
    private static readonly MethodInfo CreateTargetMethod = typeof(RabbitMqMessageTopologyCompiler)
       .GetMethod(nameof(CreateTargetCore), BindingFlags.Static | BindingFlags.NonPublic)!;

    public static RabbitMqCompiledTopology Compile(IServiceProvider serviceProvider)
    {
        if (serviceProvider is null)
        {
            throw new ArgumentNullException(nameof(serviceProvider));
        }

        var configuration = serviceProvider.GetRequiredService<RabbitMqPublishingConfiguration>();
        var validationErrors = Validate(serviceProvider, configuration);

        if (validationErrors.Count > 0)
        {
            throw new MessageTopologyValidationException(validationErrors);
        }

        var connectionManager = serviceProvider.GetRequiredService<RabbitMqConnectionManager>();
        Dictionary<Type, Target> defaultTargetsByMessageType = new ();
        Dictionary<string, Target> targetsByName = new (StringComparer.Ordinal);
        List<Target> targets = [];
        IRabbitMqChannelPool? sharedChannelPool = null;

        LogWorstCaseChannelCount(serviceProvider, configuration);

        foreach (var route in OrderRoutes(configuration.Routes))
        {
            sharedChannelPool ??= configuration.ChannelPoolingMode == RabbitMqChannelPoolingMode.Shared ?
                CreateChannelPool(connectionManager, configuration.SharedChannelPoolSize) :
                null;
            var channelPool = sharedChannelPool ??
                              CreateChannelPool(connectionManager, configuration.MaxChannelsPerTarget);
            var ownsChannelPool = configuration.ChannelPoolingMode == RabbitMqChannelPoolingMode.PerTarget;
            var target = CreateTarget(route, serviceProvider, channelPool, ownsChannelPool);
            targets.Add(target);

            if (string.IsNullOrWhiteSpace(route.TargetName))
            {
                defaultTargetsByMessageType.Add(route.MessageType, target);
            }
            else
            {
                targetsByName.Add(route.TargetName!, target);
            }
        }

        return new RabbitMqCompiledTopology(
            new MessageTopology(defaultTargetsByMessageType, targetsByName),
            configuration.Exchanges,
            configuration.Queues,
            configuration.Bindings,
            targets,
            sharedChannelPool
        );
    }

    private static IEnumerable<RabbitMqPublishRouteConfiguration> OrderRoutes(
        IReadOnlyList<RabbitMqPublishRouteConfiguration> routes
    )
    {
        return routes
           .OrderBy(static route => route.MessageType.AssemblyQualifiedName, StringComparer.Ordinal)
           .ThenBy(static route => route.TargetName ?? string.Empty, StringComparer.Ordinal);
    }

    private static Target CreateTarget(
        RabbitMqPublishRouteConfiguration route,
        IServiceProvider serviceProvider,
        IRabbitMqChannelPool channelPool,
        bool ownsChannelPool
    )
    {
        var closedMethod = CreateTargetMethod.MakeGenericMethod(route.MessageType);
        return (Target) closedMethod.Invoke(null, [route, serviceProvider, channelPool, ownsChannelPool])!;
    }

    private static Target CreateTargetCore<TMessage>(
        RabbitMqPublishRouteConfiguration route,
        IServiceProvider serviceProvider,
        IRabbitMqChannelPool channelPool,
        bool ownsChannelPool
    )
    {
        var serializer = (IMessageSerializer) serviceProvider.GetRequiredService(route.SerializerType!);
        var targetName = string.IsNullOrWhiteSpace(route.TargetName) ?
            route.MessageType.FullName ?? route.MessageType.Name :
            route.TargetName!;

        return route switch
        {
            RabbitMqFanoutPublishRouteConfiguration fanoutRoute => new RabbitMqFanoutTarget<TMessage>(
                targetName,
                serializer,
                channelPool,
                ownsChannelPool,
                fanoutRoute.ExchangeName,
                fanoutRoute.IsMandatory
            ),
            RabbitMqDirectPublishRouteConfiguration directRoute => new RabbitMqDirectTarget<TMessage>(
                targetName,
                serializer,
                channelPool,
                ownsChannelPool,
                directRoute.ExchangeName,
                directRoute.IsMandatory,
                CreateRoutingKeyFactory<TMessage>(directRoute)
            ),
            RabbitMqTopicPublishRouteConfiguration topicRoute => new RabbitMqTopicTarget<TMessage>(
                targetName,
                serializer,
                channelPool,
                ownsChannelPool,
                topicRoute.ExchangeName,
                topicRoute.IsMandatory,
                CreateRoutingKeyFactory<TMessage>(topicRoute)
            ),
            RabbitMqHeadersPublishRouteConfiguration headersRoute => new RabbitMqHeadersTarget<TMessage>(
                targetName,
                serializer,
                channelPool,
                ownsChannelPool,
                headersRoute.ExchangeName,
                headersRoute.IsMandatory,
                headersRoute.Headers
            ),
            _ => throw new ArgumentOutOfRangeException(nameof(route), route, "Unsupported RabbitMQ publish route.")
        };
    }

    private static Func<TMessage, string> CreateRoutingKeyFactory<TMessage>(
        RabbitMqRoutingKeyPublishRouteConfiguration route
    )
    {
        if (route.RoutingKeyFactory is Func<TMessage, string> typedRoutingKeyFactory)
        {
            return typedRoutingKeyFactory;
        }

        if (route.RoutingKey is not null)
        {
            var routingKey = route.RoutingKey;
            return _ => routingKey;
        }

        throw new ArgumentException("A routing-key route must provide either a constant key or a key factory.");
    }

    private static List<string> Validate(
        IServiceProvider serviceProvider,
        RabbitMqPublishingConfiguration configuration
    )
    {
        List<string> validationErrors = [];

        if (configuration.ConnectionFactoryFactory is null)
        {
            validationErrors.Add("A RabbitMQ connection factory must be configured.");
        }

        if (!Enum.IsDefined(typeof(RabbitMqChannelPoolingMode), configuration.ChannelPoolingMode))
        {
            validationErrors.Add(
                $"RabbitMQ channel pooling mode '{configuration.ChannelPoolingMode}' is unsupported."
            );
        }

        if (configuration.MaxChannelsPerTarget < 1)
        {
            validationErrors.Add("RabbitMQ max channels per target must be greater than zero.");
        }

        if (configuration.SharedChannelPoolSize < 1)
        {
            validationErrors.Add("RabbitMQ shared channel pool size must be greater than zero.");
        }

        validationErrors.AddRange(
            FindDuplicateNames(configuration.Exchanges.Select(static exchange => exchange.Name), "exchange")
        );
        validationErrors.AddRange(FindDuplicateNames(configuration.Queues.Select(static queue => queue.Name), "queue"));
        validationErrors.AddRange(
            FindDuplicateNames(
                configuration.Routes.Where(static route => !string.IsNullOrWhiteSpace(route.TargetName))
                   .Select(static route => route.TargetName!),
                "target"
            )
        );

        var exchangesByName = configuration.Exchanges
           .GroupBy(static exchange => exchange.Name, StringComparer.Ordinal)
           .Select(static group => group.First())
           .ToDictionary(static exchange => exchange.Name, StringComparer.Ordinal);
        var queuesByName = configuration.Queues
           .GroupBy(static queue => queue.Name, StringComparer.Ordinal)
           .Select(static group => group.First())
           .ToDictionary(static queue => queue.Name, StringComparer.Ordinal);

        ValidateExchangeDefinitions(configuration.Exchanges, validationErrors);
        ValidateQueueDefinitions(configuration.Queues, validationErrors);
        ValidateRoutes(serviceProvider, configuration.Routes, exchangesByName, validationErrors);
        ValidateBindings(configuration.Bindings, exchangesByName, queuesByName, validationErrors);

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
                validationErrors.Add(
                    $"Exchange '{exchange.Name}' uses unsupported exchange type 'internal'."
                );
            }
        }
    }

    private static void ValidateQueueDefinitions(
        IReadOnlyList<RabbitMqQueueDefinition> queues,
        ICollection<string> validationErrors
    )
    {
        foreach (var queue in queues.OrderBy(static queue => queue.Name, StringComparer.Ordinal))
        {
            if (!Enum.IsDefined(typeof(RabbitMqDeclareMode), queue.DeclareMode))
            {
                validationErrors.Add(
                    $"Queue '{queue.Name}' uses unsupported declare mode '{queue.DeclareMode}'."
                );
            }
        }
    }

    private static void ValidateRoutes(
        IServiceProvider serviceProvider,
        IReadOnlyList<RabbitMqPublishRouteConfiguration> routes,
        IReadOnlyDictionary<string, RabbitMqExchangeDefinition> exchangesByName,
        ICollection<string> validationErrors
    )
    {
        foreach (var group in routes.GroupBy(
                         static route => route.MessageType.AssemblyQualifiedName!,
                         StringComparer.Ordinal
                     )
                    .OrderBy(static group => group.Key, StringComparer.Ordinal))
        {
            var unnamedRouteCount = group.Count(static route => string.IsNullOrWhiteSpace(route.TargetName));
            var messageType = group.First().MessageType;
            var messageTypeName = messageType.FullName ?? messageType.Name;

            if (unnamedRouteCount > 1)
            {
                validationErrors.Add(
                    $"Message '{messageTypeName}' configures multiple default RabbitMQ publish routes."
                );
            }

            foreach (var route in group
                        .OrderBy(static route => route.TargetName ?? string.Empty, StringComparer.Ordinal))
            {
                var routeDescription = GetRouteDescription(route);

                if (!exchangesByName.TryGetValue(route.ExchangeName, out var exchange))
                {
                    validationErrors.Add($"{routeDescription} references unknown exchange '{route.ExchangeName}'.");
                }
                else
                {
                    ValidateRouteAgainstExchange(route, exchange, routeDescription, validationErrors);
                }

                if (route.SerializerType is null)
                {
                    validationErrors.Add($"{routeDescription} must configure a serializer.");
                }
                else if (!typeof(IMessageSerializer).IsAssignableFrom(route.SerializerType))
                {
                    validationErrors.Add(
                        $"Serializer '{route.SerializerType}' for {routeDescription.ToLowerInvariant()} does not implement '{typeof(IMessageSerializer)}'."
                    );
                }
                else if (serviceProvider.GetService(route.SerializerType) is null)
                {
                    validationErrors.Add(
                        $"Serializer '{route.SerializerType}' for {routeDescription.ToLowerInvariant()} is not registered in the service provider."
                    );
                }

                if (route is RabbitMqRoutingKeyPublishRouteConfiguration routingKeyRoute)
                {
                    ValidateRoutingKeyConfiguration(routingKeyRoute, routeDescription, validationErrors);
                }
            }
        }
    }

    private static void ValidateRouteAgainstExchange(
        RabbitMqPublishRouteConfiguration route,
        RabbitMqExchangeDefinition exchange,
        string routeDescription,
        ICollection<string> validationErrors
    )
    {
        var expectedExchangeType = route switch
        {
            RabbitMqFanoutPublishRouteConfiguration => ExchangeType.Fanout,
            RabbitMqDirectPublishRouteConfiguration => ExchangeType.Direct,
            RabbitMqTopicPublishRouteConfiguration => ExchangeType.Topic,
            RabbitMqHeadersPublishRouteConfiguration => ExchangeType.Headers,
            _ => string.Empty
        };

        if (!string.Equals(exchange.Type, expectedExchangeType, StringComparison.Ordinal))
        {
            validationErrors.Add(
                $"{routeDescription} targets exchange '{exchange.Name}' of type '{exchange.Type}', but requires '{expectedExchangeType}'."
            );
        }
    }

    private static void ValidateRoutingKeyConfiguration(
        RabbitMqRoutingKeyPublishRouteConfiguration route,
        string routeDescription,
        ICollection<string> validationErrors
    )
    {
        var hasRoutingKey = route.RoutingKey is not null;
        var hasRoutingKeyFactory = route.RoutingKeyFactory is not null;

        if (hasRoutingKey == hasRoutingKeyFactory)
        {
            validationErrors.Add(
                $"{routeDescription} must configure either a constant routing key or a routing-key factory."
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
            if (!Enum.IsDefined(typeof(RabbitMqBindingDeclareMode), binding.DeclareMode))
            {
                validationErrors.Add(
                    $"{GetBindingDescription(binding)} uses unsupported declare mode '{binding.DeclareMode}'."
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

    private static IEnumerable<string> FindDuplicateNames(IEnumerable<string> names, string entityDescription)
    {
        return names
           .GroupBy(static name => name, StringComparer.Ordinal)
           .Where(static group => group.Count() > 1)
           .Select(group => $"Duplicate {entityDescription} '{group.Key}' is configured.");
    }

    private static string GetRouteDescription(RabbitMqPublishRouteConfiguration route)
    {
        var messageTypeName = route.MessageType.FullName ?? route.MessageType.Name;

        return string.IsNullOrWhiteSpace(route.TargetName) ?
            $"Publish route for message '{messageTypeName}'" :
            $"Publish route for message '{messageTypeName}' and target '{route.TargetName}'";
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

    private static DefaultRabbitMqChannelPool CreateChannelPool(
        RabbitMqConnectionManager connectionManager,
        int maximumChannelCount
    )
    {
        return new DefaultRabbitMqChannelPool(
            maximumChannelCount,
            async cancellationToken =>
            {
                var connection = await connectionManager.GetConnectionAsync(cancellationToken).ConfigureAwait(false);
                return await connection.CreateChannelAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
            }
        );
    }

    private static void LogWorstCaseChannelCount(
        IServiceProvider serviceProvider,
        RabbitMqPublishingConfiguration configuration
    )
    {
        var worstCaseChannelCount = GetWorstCaseChannelCount(configuration);
        var loggerFactory = serviceProvider.GetService<ILoggerFactory>() ?? NullLoggerFactory.Instance;
        var logger = loggerFactory.CreateLogger(typeof(RabbitMqMessageTopologyCompiler));
        logger.LogInformation(
            "RabbitMQ publish topology may open up to {ChannelCount} channels ({Description}).",
            worstCaseChannelCount,
            GetWorstCaseChannelCountDescription(configuration)
        );
    }

    private static int GetWorstCaseChannelCount(RabbitMqPublishingConfiguration configuration)
    {
        if (configuration.Routes.Count == 0)
        {
            return 0;
        }

        return configuration.ChannelPoolingMode switch
        {
            RabbitMqChannelPoolingMode.PerTarget =>
                checked(configuration.Routes.Count * configuration.MaxChannelsPerTarget),
            RabbitMqChannelPoolingMode.Shared => configuration.SharedChannelPoolSize,
            _ => 0
        };
    }

    private static string GetWorstCaseChannelCountDescription(RabbitMqPublishingConfiguration configuration)
    {
        return configuration.ChannelPoolingMode switch
        {
            RabbitMqChannelPoolingMode.PerTarget =>
                $"PerTarget mode, {configuration.Routes.Count} targets × max {configuration.MaxChannelsPerTarget}",
            RabbitMqChannelPoolingMode.Shared =>
                $"Shared mode, shared pool size {configuration.SharedChannelPoolSize}",
            _ => "unknown pooling mode"
        };
    }
}
