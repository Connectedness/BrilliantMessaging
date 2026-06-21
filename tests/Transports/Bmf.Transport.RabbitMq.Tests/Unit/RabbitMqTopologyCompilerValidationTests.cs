using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Bmf.Core.Messaging;
using Bmf.Core.Messaging.Inbound;
using Bmf.Core.Messaging.Outbound;
using Bmf.Transport.RabbitMq.Inbound;
using Bmf.Transport.RabbitMq.Outbound;
using Bmf.Transport.RabbitMq.Tests.TestSupport;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using Xunit;

namespace Bmf.Transport.RabbitMq.Tests.Unit;

public sealed class RabbitMqTopologyCompilerValidationTests
{
    [Fact]
    public void Compile_ReportsInvalidDefinitionRecordsThatFluentBuildersRejectEarlier()
    {
        var configuration = new RabbitMqTopologyConfiguration(
            static _ => new ConnectionFactory(),
            [
                new RabbitMqExchangeDefinition(
                    "bad-exchange",
                    ExchangeType.Fanout,
                    (RabbitMqDeclareMode) 999,
                    true,
                    false,
                    new Dictionary<string, object?>()
                )
            ],
            [
                new RabbitMqQueueDefinition(
                    "bad-queue",
                    (RabbitMqDeclareMode) 999,
                    true,
                    false,
                    false,
                    new Dictionary<string, object?>()
                )
            ],
            [],
            [
                new RabbitMqOutboundChannelGroupDefinition(
                    "$implicit:bad-outbound",
                    0,
                    (RabbitMqPublisherConfirmMode) 999,
                    TimeSpan.Zero
                )
            ],
            [
                new RabbitMqFanoutOutboundTargetDefinition(
                    typeof(ValidationMessageA),
                    "bad-exchange",
                    "missing-outbound",
                    "invalid-serializer",
                    typeof(string),
                    true
                )
            ],
            [
                new RabbitMqInboundChannelGroupDefinition(
                    "$implicit:bad-inbound",
                    0,
                    0,
                    0
                )
            ],
            [
                new RabbitMqInboundConsumerDefinition(
                    "missing-queue",
                    typeof(string),
                    "missing-inbound",
                    1,
                    1,
                    1,
                    true,
                    [
                        new RabbitMqInboundHandlerDefinition(
                            null,
                            typeof(UnregisteredMessage),
                            typeof(RegisteredHandler),
                            static _ => Task.CompletedTask,
                            typeof(string),
                            (MessageAckMode) 999
                        )
                    ]
                )
            ],
            typeof(string),
            ConfigurePipeline: null,
            ShutdownTimeout: TimeSpan.FromSeconds(1),
            (RabbitMqPublisherConfirmMode) 999,
            TimeSpan.Zero
        );
        RabbitMqTopologyCompiler compiler = new (
            RabbitMqCloudEventsTestFactory.CreateRegistry(),
            NullLoggerFactory.Instance,
            static type => type == typeof(CloudEventMessageSerializer) ?
                RabbitMqCloudEventsTestFactory.CreateSerializer() :
                null,
            static type => type == typeof(RegisteredHandler)
        );
        RabbitMqConnectionProvider connectionProvider = new (
            static _ => Task.FromException<IConnection>(new NotSupportedException())
        );

        Action act = () => _ = compiler.Compile(Topology.DefaultName, configuration, connectionProvider);

        act.Should().Throw<TopologyValidationException>()
           .Which.ValidationErrors.Should().BeEquivalentTo(
                "Exchange 'bad-exchange' uses unsupported declare mode '999'.",
                "Queue 'bad-queue' uses unsupported declare mode '999'.",
                "Channel group '$implicit:bad-outbound' maximum channel count must be greater than zero.",
                "Channel group '$implicit:bad-outbound' uses reserved name prefix '$implicit:'.",
                "Channel group '$implicit:bad-outbound' uses unsupported publisher confirm mode '999'.",
                "Channel group '$implicit:bad-outbound' publisher confirm timeout must be finite and greater than zero.",
                "Channel group '$implicit:bad-outbound' is configured but no outbound target references it.",
                "Outbound target for message 'Bmf.Transport.RabbitMq.Tests.TestSupport.ValidationMessageA' and target 'invalid-serializer' references unknown channel group 'missing-outbound'.",
                $"Serializer '{typeof(string)}' for outbound target for message 'bmf.transport.rabbitmq.tests.testsupport.validationmessagea' and target 'invalid-serializer' does not implement '{typeof(IMessageSerializer)}'.",
                "RabbitMQ outbound topology uses unsupported default publisher confirm mode '999'.",
                "RabbitMQ outbound topology publisher confirm timeout must be finite and greater than zero.",
                "Channel group '$implicit:bad-inbound' maximum channel count must be greater than zero.",
                "Channel group '$implicit:bad-inbound' prefetch count must be greater than zero.",
                "Channel group '$implicit:bad-inbound' consumer dispatch concurrency must be greater than zero.",
                "Channel group '$implicit:bad-inbound' uses reserved name prefix '$implicit:'.",
                "Channel group '$implicit:bad-inbound' is configured but no inbound endpoint references it.",
                $"Inbound deserialization middleware '{typeof(string)}' must implement '{typeof(IMessageMiddleware)}'.",
                "Inbound consumer references unknown queue 'missing-queue'.",
                "Inbound consumer for queue 'missing-queue' references unknown channel group 'missing-inbound'.",
                $"Inbound inspector '{typeof(string)}' for queue 'missing-queue' does not implement '{typeof(IInboundMessageInspector)}'.",
                $"Inbound deserializer '{typeof(string)}' for message '{typeof(UnregisteredMessage)}' does not implement '{typeof(IMessageDeserializer)}'.",
                $"Inbound endpoint for message '{typeof(UnregisteredMessage)}' uses unsupported acknowledgement mode '999'.",
                $"Inbound endpoint for message '{typeof(UnregisteredMessage)}' consumes unregistered CloudEvents message type. Register its canonical discriminator with MessageContractRegistryBuilder.Map<T>(...)."
            );
    }

    // ReSharper disable once NotAccessedPositionalProperty.Local -- required for testing scenario
    private sealed record UnregisteredMessage(string Value);

    private sealed class RegisteredHandler : IMessageHandler<UnregisteredMessage>
    {
        public Task HandleAsync(
            UnregisteredMessage message,
            IncomingMessageContext context,
            CancellationToken cancellationToken
        )
        {
            return Task.CompletedTask;
        }
    }
}
