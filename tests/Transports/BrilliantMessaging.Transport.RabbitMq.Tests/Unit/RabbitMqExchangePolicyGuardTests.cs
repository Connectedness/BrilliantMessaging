using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;
using BrilliantMessaging.Core.Messaging.Outbound;
using BrilliantMessaging.Transport.RabbitMq.Outbound;
using BrilliantMessaging.Transport.RabbitMq.Tests.TestSupport;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using Xunit;

namespace BrilliantMessaging.Transport.RabbitMq.Tests.Unit;

public sealed class RabbitMqExchangePolicyGuardTests
{
    [Fact]
    public void Compile_RejectsTargetPublishingToDeleteModeExchange()
    {
        var configuration = new RabbitMqTopologyConfiguration(
            static _ => new ConnectionFactory(),
            [
                new RabbitMqExchangeDefinition(
                    "doomed",
                    ExchangeType.Direct,
                    RabbitMqDeclareMode.Delete,
                    true,
                    false,
                    new Dictionary<string, object?>()
                )
            ],
            [],
            [],
            [],
            [
                new RabbitMqDirectOutboundTargetDefinition(
                    typeof(ValidationMessageA),
                    "doomed",
                    null,
                    null,
                    typeof(CloudEventMessageSerializer),
                    false,
                    "routing",
                    null
                )
            ],
            [],
            [],
            typeof(MessageDeserializationMiddleware),
            ConfigurePipeline: null,
            ShutdownTimeout: TimeSpan.FromSeconds(1)
        );

        var errors = CompileAndCollectErrors(configuration);

        errors.Should().Contain(
            "Outbound target for message 'BrilliantMessaging.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' references exchange 'doomed' declared with Delete mode; remove the target or change the exchange's declare mode."
        );
    }

    [Fact]
    public void Compile_AllowsTargetPublishingToActiveExchangeWithSameDeleteExchangePresent()
    {
        // A Delete-mode exchange on its own (with no target referencing it) is fine — the guard only fires when
        // a target actually names the Delete exchange.
        var configuration = new RabbitMqTopologyConfiguration(
            static _ => new ConnectionFactory(),
            [
                new RabbitMqExchangeDefinition(
                    "active",
                    ExchangeType.Direct,
                    RabbitMqDeclareMode.Active,
                    true,
                    false,
                    new Dictionary<string, object?>()
                ),
                new RabbitMqExchangeDefinition(
                    "doomed",
                    ExchangeType.Direct,
                    RabbitMqDeclareMode.Delete,
                    true,
                    false,
                    new Dictionary<string, object?>()
                )
            ],
            [],
            [],
            [],
            [
                new RabbitMqDirectOutboundTargetDefinition(
                    typeof(ValidationMessageA),
                    "active",
                    null,
                    null,
                    typeof(CloudEventMessageSerializer),
                    false,
                    "routing",
                    null
                )
            ],
            [],
            [],
            typeof(MessageDeserializationMiddleware),
            ConfigurePipeline: null,
            ShutdownTimeout: TimeSpan.FromSeconds(1)
        );

        var errors = CompileAndCollectErrors(configuration);

        errors.Should().NotContain(e => e.Contains("declared with Delete mode"));
    }

    [Fact]
    public void Compile_RejectsHeaderMatchOnQueueBindingWithDirectSourceExchange()
    {
        var configuration = CreateConfigurationWithBinding(
            sourceExchangeType: ExchangeType.Direct,
            binding: new RabbitMqQueueBindingDefinition(
                "source",
                "queue",
                "routing",
                RabbitMqBindingMode.Active,
                new Dictionary<string, object?> { ["x-match"] = "all", ["tenant"] = "acme" }
            )
        );

        var errors = CompileAndCollectErrors(configuration);

        errors.Should().Contain(
            "Queue binding from exchange 'source' to queue 'queue' configures header-match arguments but its source exchange 'source' is of type 'direct'. Header matching requires a headers exchange; remove the header-match configuration or use a headers exchange."
        );
    }

    [Fact]
    public void Compile_RejectsHeaderMatchOnQueueBindingWithTopicSourceExchange()
    {
        var configuration = CreateConfigurationWithBinding(
            sourceExchangeType: ExchangeType.Topic,
            binding: new RabbitMqQueueBindingDefinition(
                "source",
                "queue",
                "routing",
                RabbitMqBindingMode.Active,
                new Dictionary<string, object?> { ["x-match"] = "any" }
            )
        );

        var errors = CompileAndCollectErrors(configuration);

        errors.Should().Contain(
            "Queue binding from exchange 'source' to queue 'queue' configures header-match arguments but its source exchange 'source' is of type 'topic'. Header matching requires a headers exchange; remove the header-match configuration or use a headers exchange."
        );
    }

    [Fact]
    public void Compile_RejectsHeaderMatchOnExchangeBindingWithDirectSourceExchange()
    {
        var configuration = CreateConfigurationWithBinding(
            sourceExchangeType: ExchangeType.Direct,
            binding: new RabbitMqExchangeBindingDefinition(
                "source",
                "destination",
                "routing",
                RabbitMqBindingMode.Active,
                new Dictionary<string, object?> { ["x-match"] = "all", ["tenant"] = "acme" }
            ),
            destinationExchangeType: ExchangeType.Direct
        );

        var errors = CompileAndCollectErrors(configuration);

        errors.Should().Contain(
            "Exchange binding from exchange 'source' to exchange 'destination' configures header-match arguments but its source exchange 'source' is of type 'direct'. Header matching requires a headers exchange; remove the header-match configuration or use a headers exchange."
        );
    }

    [Fact]
    public void Compile_AllowsHeaderMatchOnQueueBindingWithHeadersSourceExchange()
    {
        var configuration = CreateConfigurationWithBinding(
            sourceExchangeType: ExchangeType.Headers,
            binding: new RabbitMqQueueBindingDefinition(
                "source",
                "queue",
                "",
                RabbitMqBindingMode.Active,
                new Dictionary<string, object?> { ["x-match"] = "all-with-x", ["x-tenant"] = "acme" }
            )
        );

        var errors = CompileAndCollectErrors(configuration);

        errors.Should().NotContain(e => e.Contains("header-match"));
    }

    [Fact]
    public void Compile_AllowsHeaderMatchOnExchangeBindingWithHeadersSourceExchange()
    {
        var configuration = CreateConfigurationWithBinding(
            sourceExchangeType: ExchangeType.Headers,
            binding: new RabbitMqExchangeBindingDefinition(
                "source",
                "destination",
                "",
                RabbitMqBindingMode.Active,
                new Dictionary<string, object?> { ["x-match"] = "any", ["tenant"] = "acme" }
            ),
            destinationExchangeType: ExchangeType.Direct
        );

        var errors = CompileAndCollectErrors(configuration);

        errors.Should().NotContain(e => e.Contains("header-match"));
    }

    [Fact]
    public void Compile_RejectsPlainAllWithXPrefixedHeaderPredicateOnHeadersQueueBinding()
    {
        var binding = ((IBuildable<RabbitMqQueueBindingDefinition>) new RabbitMqQueueBindingBuilder(
                "source",
                "queue",
                ""
            )
           .WithHeader("x-tenant", "acme")).Build();
        var configuration = CreateConfigurationWithBinding(
            sourceExchangeType: ExchangeType.Headers,
            binding: binding
        );

        var errors = CompileAndCollectErrors(configuration);

        errors.Should().Contain(
            "Queue binding from exchange 'source' to queue 'queue' configures x-prefixed header predicate(s) 'x-tenant' with x-match 'all'. RabbitMQ ignores x-prefixed header predicates for plain 'all' and 'any' matches; use WithHeaderMatch(RabbitMqHeaderMatch.AllWithX or RabbitMqHeaderMatch.AnyWithX) or remove the x-prefixed predicate."
        );
    }

    [Fact]
    public void Compile_RejectsPlainAnyWithXPrefixedHeaderPredicateOnHeadersExchangeBinding()
    {
        var configuration = CreateConfigurationWithBinding(
            sourceExchangeType: ExchangeType.Headers,
            binding: new RabbitMqExchangeBindingDefinition(
                "source",
                "destination",
                "",
                RabbitMqBindingMode.Active,
                new Dictionary<string, object?> { ["x-match"] = "any", ["x-tenant"] = "acme" }
            ),
            destinationExchangeType: ExchangeType.Direct
        );

        var errors = CompileAndCollectErrors(configuration);

        errors.Should().Contain(
            "Exchange binding from exchange 'source' to exchange 'destination' configures x-prefixed header predicate(s) 'x-tenant' with x-match 'any'. RabbitMQ ignores x-prefixed header predicates for plain 'all' and 'any' matches; use WithHeaderMatch(RabbitMqHeaderMatch.AllWithX or RabbitMqHeaderMatch.AnyWithX) or remove the x-prefixed predicate."
        );
    }

    [Fact]
    public void Compile_RejectsXPrefixedHeaderPredicateWithoutHeaderMatchOnHeadersBinding()
    {
        var configuration = CreateConfigurationWithBinding(
            sourceExchangeType: ExchangeType.Headers,
            binding: new RabbitMqQueueBindingDefinition(
                "source",
                "queue",
                "",
                RabbitMqBindingMode.Active,
                new Dictionary<string, object?> { ["x-tenant"] = "acme" }
            )
        );

        var errors = CompileAndCollectErrors(configuration);

        errors.Should().Contain(
            "Queue binding from exchange 'source' to queue 'queue' configures x-prefixed header predicate(s) 'x-tenant' without an x-match argument. RabbitMQ's plain/default header-match modes ignore x-prefixed header predicates; use WithHeaderMatch(RabbitMqHeaderMatch.AllWithX or RabbitMqHeaderMatch.AnyWithX) or remove the x-prefixed predicate."
        );
    }

    [Fact]
    public void Compile_AllowsAnyWithXWithXPrefixedHeaderPredicateOnHeadersExchangeBinding()
    {
        var configuration = CreateConfigurationWithBinding(
            sourceExchangeType: ExchangeType.Headers,
            binding: new RabbitMqExchangeBindingDefinition(
                "source",
                "destination",
                "",
                RabbitMqBindingMode.Active,
                new Dictionary<string, object?> { ["x-match"] = "any-with-x", ["x-tenant"] = "acme" }
            ),
            destinationExchangeType: ExchangeType.Direct
        );

        var errors = CompileAndCollectErrors(configuration);

        errors.Should().NotContain(e => e.Contains("x-prefixed header predicate"));
    }

    [Fact]
    public void Compile_RejectsUnsupportedHeaderMatchValueOnActiveHeadersBinding()
    {
        var configuration = CreateConfigurationWithBinding(
            sourceExchangeType: ExchangeType.Headers,
            binding: new RabbitMqQueueBindingDefinition(
                "source",
                "queue",
                "",
                RabbitMqBindingMode.Active,
                new Dictionary<string, object?> { ["x-match"] = "sometimes", ["tenant"] = "acme" }
            )
        );

        var errors = CompileAndCollectErrors(configuration);

        errors.Should().Contain(
            "Queue binding from exchange 'source' to queue 'queue' configures unsupported headers-exchange x-match value 'sometimes'. Supported values are 'all', 'any', 'all-with-x', and 'any-with-x'."
        );
    }

    [Fact]
    public void Compile_RejectsNonStringHeaderMatchValueOnActiveHeadersBinding()
    {
        var configuration = CreateConfigurationWithBinding(
            sourceExchangeType: ExchangeType.Headers,
            binding: new RabbitMqQueueBindingDefinition(
                "source",
                "queue",
                "",
                RabbitMqBindingMode.Active,
                new Dictionary<string, object?> { ["x-match"] = 1, ["tenant"] = "acme" }
            )
        );

        var errors = CompileAndCollectErrors(configuration);

        errors.Should().Contain(
            "Queue binding from exchange 'source' to queue 'queue' configures unsupported headers-exchange x-match value '1'. Supported values are 'all', 'any', 'all-with-x', and 'any-with-x'."
        );
    }

    [Fact]
    public void Compile_AllowsDeleteBindingWithPlainAllAndXPrefixedHeaderPredicateOnHeadersExchange()
    {
        var configuration = CreateConfigurationWithBinding(
            sourceExchangeType: ExchangeType.Headers,
            binding: new RabbitMqQueueBindingDefinition(
                "source",
                "queue",
                "",
                RabbitMqBindingMode.Delete,
                new Dictionary<string, object?> { ["x-match"] = "all", ["x-tenant"] = "acme" }
            )
        );

        var errors = CompileAndCollectErrors(configuration);

        errors.Should().NotContain(e => e.Contains("x-prefixed header predicate"));
    }

    [Fact]
    public void Compile_AllowsBindingWithoutHeaderMatchOnDirectSourceExchange()
    {
        // A binding without x-match on a non-headers exchange is the normal case and must not trigger the guard.
        var configuration = CreateConfigurationWithBinding(
            sourceExchangeType: ExchangeType.Direct,
            binding: new RabbitMqQueueBindingDefinition(
                "source",
                "queue",
                "routing",
                RabbitMqBindingMode.Active,
                new Dictionary<string, object?>()
            )
        );

        var errors = CompileAndCollectErrors(configuration);

        errors.Should().NotContain(e => e.Contains("header-match"));
    }

    [Fact]
    public void Compile_DoesNotGuardHeaderMatchWhenSourceExchangeIsUnknown()
    {
        // When the source exchange is unknown, the unknown-exchange error already fires; the header-match guard
        // skips (it only checks when the source exchange is resolved) so it does not double-report.
        var configuration = new RabbitMqTopologyConfiguration(
            static _ => new ConnectionFactory(),
            [],
            [
                new RabbitMqQueueDefinition(
                    "queue",
                    RabbitMqDeclareMode.Active,
                    true,
                    false,
                    false,
                    new Dictionary<string, object?>()
                )
            ],
            [
                new RabbitMqQueueBindingDefinition(
                    "missing-source",
                    "queue",
                    "routing",
                    RabbitMqBindingMode.Active,
                    new Dictionary<string, object?> { ["x-match"] = "all" }
                )
            ],
            [],
            [],
            [],
            [],
            typeof(MessageDeserializationMiddleware),
            ConfigurePipeline: null,
            ShutdownTimeout: TimeSpan.FromSeconds(1)
        );

        var errors = CompileAndCollectErrors(configuration);

        errors.Should().Contain(e => e.Contains("references unknown source exchange 'missing-source'"));
        errors.Should().NotContain(e => e.Contains("header-match"));
    }

    private static RabbitMqTopologyConfiguration CreateConfigurationWithBinding(
        string sourceExchangeType,
        RabbitMqBindingDefinition binding,
        string? destinationExchangeType = null
    )
    {
        List<RabbitMqExchangeDefinition> exchanges =
        [
            new (
                "source",
                sourceExchangeType,
                RabbitMqDeclareMode.Active,
                true,
                false,
                new Dictionary<string, object?>()
            )
        ];

        List<RabbitMqQueueDefinition> queues =
        [
            new (
                "queue",
                RabbitMqDeclareMode.Active,
                true,
                false,
                false,
                new Dictionary<string, object?>()
            )
        ];

        if (binding is RabbitMqExchangeBindingDefinition)
        {
            exchanges.Add(
                new RabbitMqExchangeDefinition(
                    "destination",
                    destinationExchangeType ?? ExchangeType.Direct,
                    RabbitMqDeclareMode.Active,
                    true,
                    false,
                    new Dictionary<string, object?>()
                )
            );
        }

        return new RabbitMqTopologyConfiguration(
            static _ => new ConnectionFactory(),
            exchanges,
            queues,
            [binding],
            [],
            [],
            [],
            [],
            typeof(MessageDeserializationMiddleware),
            ConfigurePipeline: null,
            ShutdownTimeout: TimeSpan.FromSeconds(1)
        );
    }

    private static IReadOnlyList<string> CompileAndCollectErrors(RabbitMqTopologyConfiguration configuration)
    {
        RabbitMqTopologyCompiler compiler = new (
            RabbitMqCloudEventsTestFactory.CreateRegistry(),
            NullLoggerFactory.Instance,
            static type => type == typeof(CloudEventMessageSerializer) ?
                RabbitMqCloudEventsTestFactory.CreateSerializer() :
                null,
            static type => type == typeof(MessageDeserializationMiddleware)
        );
        RabbitMqConnectionProvider connectionProvider = new (
            static _ => Task.FromException<IConnection>(new NotSupportedException())
        );

        try
        {
            _ = compiler.Compile(Topology.DefaultName, configuration, connectionProvider);
            return [];
        }
        catch (TopologyValidationException ex)
        {
            return ex.ValidationErrors;
        }
    }
}
