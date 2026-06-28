using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Inbound;
using BrilliantMessaging.Core.Messaging.Outbound;
using BrilliantMessaging.Transport.RabbitMq.Inbound;
using BrilliantMessaging.Transport.RabbitMq.Tests.TestSupport;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using Xunit;

namespace BrilliantMessaging.Transport.RabbitMq.Tests.Unit;

public sealed class RabbitMqQueueKnobGuardTests
{
    [Fact]
    public void Compile_RejectsQuorumOnlyDeliveryLimitOnClassicQueue()
    {
        var configuration = CreateConfiguration(
            CreateClassicQueue("work", new Dictionary<string, object?> { ["x-delivery-limit"] = 5 })
        );

        var errors = CompileAndCollectErrors(configuration);

        errors.Should().Contain(
            "Queue 'work' configures 'x-delivery-limit' but the effective queue type is 'classic'. This argument requires a quorum queue; call AsQuorumQueue() on the queue declaration or QueueType(RabbitMqQueueType.Quorum) on the consumer."
        );
    }

    [Fact]
    public void Compile_RejectsQuorumOnlyDeliveryLimitOnUnknownQueue()
    {
        var configuration = CreateConfiguration(
            CreateQueue(
                "work",
                RabbitMqDeclareMode.Passive,
                new Dictionary<string, object?> { ["x-delivery-limit"] = 5 }
            )
        );

        var errors = CompileAndCollectErrors(configuration);

        errors.Should().Contain(
            "Queue 'work' configures 'x-delivery-limit' but the effective queue type is 'unknown'. This argument requires a quorum queue; call AsQuorumQueue() on the queue declaration or QueueType(RabbitMqQueueType.Quorum) on the consumer."
        );
    }

    [Fact]
    public void Compile_AllowsQuorumOnlyKnobsOnQuorumQueueWithoutConsumer()
    {
        var configuration = CreateConfiguration(
            CreateQuorumQueue(
                "work",
                new Dictionary<string, object?>
                {
                    ["x-queue-type"] = "quorum",
                    ["x-delivery-limit"] = 3,
                    ["x-dead-letter-strategy"] = "at-most-once",
                    ["x-queue-leader-locator"] = "balanced",
                    ["x-quorum-initial-group-size"] = 3,
                    ["x-consumer-timeout"] = 30000L
                }
            )
        );

        var errors = CompileAndCollectErrors(configuration);

        errors.Should().NotContain(e => e.Contains("x-delivery-limit") && e.Contains("work"));
        errors.Should().NotContain(e => e.Contains("x-dead-letter-strategy") && e.Contains("work"));
        errors.Should().NotContain(e => e.Contains("x-queue-leader-locator") && e.Contains("work"));
        errors.Should().NotContain(e => e.Contains("x-quorum-initial-group-size") && e.Contains("work"));
        errors.Should().NotContain(e => e.Contains("x-consumer-timeout") && e.Contains("work"));
    }

    [Fact]
    public void Compile_RejectsDelayedRetryOnClassicQueue()
    {
        var configuration = CreateConfiguration(
            CreateClassicQueue(
                "work",
                new Dictionary<string, object?>
                {
                    ["x-queue-type"] = "classic",
                    ["x-delayed-retry-type"] = "all",
                    ["x-delayed-retry-min"] = 1000L
                }
            )
        );

        var errors = CompileAndCollectErrors(configuration);

        errors.Should().Contain(
            "Queue 'work' configures 'x-delayed-retry-type' but the effective queue type is 'classic'. This argument requires a quorum queue; call AsQuorumQueue() on the queue declaration or QueueType(RabbitMqQueueType.Quorum) on the consumer."
        );
        errors.Should().Contain(
            "Queue 'work' configures 'x-delayed-retry-min' but the effective queue type is 'classic'. This argument requires a quorum queue; call AsQuorumQueue() on the queue declaration or QueueType(RabbitMqQueueType.Quorum) on the consumer."
        );
    }

    [Fact]
    public void Compile_RejectsRejectPublishDlxOnQuorumQueue()
    {
        var configuration = CreateConfiguration(
            CreateQuorumQueue(
                "work",
                new Dictionary<string, object?>
                {
                    ["x-queue-type"] = "quorum",
                    ["x-overflow"] = "reject-publish-dlx"
                }
            )
        );

        var errors = CompileAndCollectErrors(configuration);

        errors.Should().Contain(
            "Queue 'work' configures 'x-overflow=reject-publish-dlx' but the effective queue type is 'quorum'. Quorum queues do not support reject-publish-dlx; use DropHead or RejectPublish, or call AsClassicQueue() to use reject-publish-dlx."
        );
    }

    [Fact]
    public void Compile_AllowsRejectPublishDlxOnClassicQueue()
    {
        var configuration = CreateConfiguration(
            CreateClassicQueue(
                "work",
                new Dictionary<string, object?>
                {
                    ["x-queue-type"] = "classic",
                    ["x-overflow"] = "reject-publish-dlx"
                }
            )
        );

        var errors = CompileAndCollectErrors(configuration);

        errors.Should().NotContain(e => e.Contains("reject-publish-dlx") && e.Contains("work"));
    }

    [Fact]
    public void Compile_RejectsMaxPriorityOnQuorumQueue()
    {
        var configuration = CreateConfiguration(
            CreateQuorumQueue(
                "work",
                new Dictionary<string, object?>
                {
                    ["x-queue-type"] = "quorum",
                    ["x-max-priority"] = (byte) 5
                }
            )
        );

        var errors = CompileAndCollectErrors(configuration);

        errors.Should().Contain(
            "Queue 'work' configures 'x-max-priority' but the effective queue type is 'quorum'. Quorum queues silently ignore x-max-priority and always use the full 0-31 priority range; call AsClassicQueue() to control the priority range."
        );
    }

    [Fact]
    public void Compile_AllowsMaxPriorityOnClassicQueue()
    {
        var configuration = CreateConfiguration(
            CreateClassicQueue(
                "work",
                new Dictionary<string, object?>
                {
                    ["x-queue-type"] = "classic",
                    ["x-max-priority"] = (byte) 5
                }
            )
        );

        var errors = CompileAndCollectErrors(configuration);

        errors.Should().NotContain(e => e.Contains("x-max-priority") && e.Contains("work"));
    }

    [Fact]
    public void Compile_AllowsQuorumKnobsOnPassiveQueueWithConsumerAssertingQuorum()
    {
        var configuration = CreateConfigurationWithConsumer(
            CreateQueue(
                "work",
                RabbitMqDeclareMode.Passive,
                new Dictionary<string, object?> { ["x-delivery-limit"] = 3 }
            ),
            consumerQueueType: RabbitMqQueueType.Quorum
        );

        var errors = CompileAndCollectErrors(configuration);

        errors.Should().NotContain(e => e.Contains("x-delivery-limit") && e.Contains("work"));
    }

    [Fact]
    public void Compile_RejectsQuorumKnobsOnPassiveQueueWithoutConsumerAssertingType()
    {
        var configuration = CreateConfiguration(
            CreateQueue(
                "work",
                RabbitMqDeclareMode.Passive,
                new Dictionary<string, object?> { ["x-delivery-limit"] = 3 }
            )
        );

        var errors = CompileAndCollectErrors(configuration);

        errors.Should().Contain(
            "Queue 'work' configures 'x-delivery-limit' but the effective queue type is 'unknown'. This argument requires a quorum queue; call AsQuorumQueue() on the queue declaration or QueueType(RabbitMqQueueType.Quorum) on the consumer."
        );
    }

    [Fact]
    public void Compile_DoesNotGuardKnobsOnDeleteModeQueue()
    {
        var configuration = CreateConfiguration(
            CreateQueue(
                "work",
                RabbitMqDeclareMode.Delete,
                new Dictionary<string, object?>
                {
                    ["x-queue-type"] = "quorum",
                    ["x-delivery-limit"] = 3,
                    ["x-max-priority"] = (byte) 5,
                    ["x-overflow"] = "reject-publish-dlx"
                }
            )
        );

        var errors = CompileAndCollectErrors(configuration);

        errors.Should().NotContain(e => e.Contains("x-delivery-limit"));
        errors.Should().NotContain(e => e.Contains("x-max-priority"));
        errors.Should().NotContain(e => e.Contains("reject-publish-dlx"));
    }

    [Fact]
    public void Compile_RejectsConsumerOnDeleteModeQueue()
    {
        var configuration = new RabbitMqTopologyConfiguration(
            static _ => new ConnectionFactory(),
            [],
            [
                new RabbitMqQueueDefinition(
                    "doomed",
                    RabbitMqDeclareMode.Delete,
                    true,
                    false,
                    false,
                    new Dictionary<string, object?> { ["x-queue-type"] = "quorum" }
                )
            ],
            [],
            [],
            [],
            [],
            [
                new RabbitMqInboundConsumerDefinition(
                    "doomed",
                    ImmutableArray<InboundMessageInspectorChainEntry>.Empty,
                    null,
                    1,
                    1,
                    1,
                    true,
                    ImmutableArray.Create(
                        new RabbitMqInboundHandlerDefinition(
                            null,
                            typeof(ValidationMessageA),
                            typeof(KnobGuardValidationMessageAHandler),
                            static _ => Task.CompletedTask,
                            typeof(PayloadCodecMessageDeserializer),
                            MessageAckMode.Auto
                        )
                    )
                )
            ],
            typeof(MessageDeserializationMiddleware),
            ConfigurePipeline: null,
            ShutdownTimeout: TimeSpan.FromSeconds(1)
        );

        var errors = CompileAndCollectErrors(configuration);

        errors.Should().Contain(
            "Inbound consumer for queue 'doomed' references a queue declared with Delete mode; remove the Consume(…) call or change the queue's declare mode."
        );
    }

    private static RabbitMqQueueDefinition CreateClassicQueue(
        string name,
        Dictionary<string, object?> arguments
    )
    {
        arguments["x-queue-type"] = "classic";

        return new RabbitMqQueueDefinition(name, RabbitMqDeclareMode.Active, true, false, false, arguments);
    }

    private static RabbitMqQueueDefinition CreateQuorumQueue(
        string name,
        Dictionary<string, object?> arguments
    )
    {
        arguments["x-queue-type"] = "quorum";

        return new RabbitMqQueueDefinition(name, RabbitMqDeclareMode.Active, true, false, false, arguments);
    }

    private static RabbitMqQueueDefinition CreateQueue(
        string name,
        RabbitMqDeclareMode declareMode,
        Dictionary<string, object?> arguments
    )
    {
        return new RabbitMqQueueDefinition(name, declareMode, true, false, false, arguments);
    }

    private static RabbitMqTopologyConfiguration CreateConfiguration(params RabbitMqQueueDefinition[] queues)
    {
        return new RabbitMqTopologyConfiguration(
            static _ => new ConnectionFactory(),
            [],
            queues,
            [],
            [],
            [],
            [],
            [],
            typeof(MessageDeserializationMiddleware),
            ConfigurePipeline: null,
            ShutdownTimeout: TimeSpan.FromSeconds(1)
        );
    }

    private static RabbitMqTopologyConfiguration CreateConfigurationWithConsumer(
        RabbitMqQueueDefinition queue,
        RabbitMqQueueType consumerQueueType
    )
    {
        return new RabbitMqTopologyConfiguration(
            static _ => new ConnectionFactory(),
            [],
            [queue],
            [],
            [],
            [],
            [],
            [
                new RabbitMqInboundConsumerDefinition(
                    queue.Name,
                    ImmutableArray<InboundMessageInspectorChainEntry>.Empty,
                    null,
                    1,
                    1,
                    1,
                    true,
                    ImmutableArray.Create(
                        new RabbitMqInboundHandlerDefinition(
                            null,
                            typeof(ValidationMessageA),
                            typeof(KnobGuardValidationMessageAHandler),
                            static _ => Task.CompletedTask,
                            typeof(PayloadCodecMessageDeserializer),
                            MessageAckMode.Auto
                        )
                    ),
                    QueueType: consumerQueueType
                )
            ],
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
            static type => type == typeof(KnobGuardValidationMessageAHandler) ||
                           type == typeof(PayloadCodecMessageDeserializer) ||
                           type == typeof(MessageDeserializationMiddleware)
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

    private sealed class KnobGuardValidationMessageAHandler : IMessageHandler<ValidationMessageA>
    {
        public Task HandleAsync(
            ValidationMessageA message,
            IncomingMessageContext context,
            CancellationToken cancellationToken
        )
        {
            return Task.CompletedTask;
        }
    }
}
