using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Transport.RabbitMq.Tests.TestSupport;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using Testcontainers.RabbitMq;
using Xunit;

namespace BrilliantMessaging.Transport.RabbitMq.Tests.Integration;

[Collection<RabbitMqCollection>]
public sealed class RabbitMqExchangePolicyIntegrationTests
{
    private readonly RabbitMqContainer _container;

    public RabbitMqExchangePolicyIntegrationTests(RabbitMqFixture fixture)
    {
        _container = fixture.Container;
    }

    [Fact]
    public async Task DeleteExchange_CascadesToDestinationExchangeBindingsAndIsIdempotent()
    {
        const string upstream = "policy-xcascade-upstream";
        const string downstream = "policy-xcascade-downstream";
        const string queue = "policy-xcascade-queue";
        var cancellationToken = TestContext.Current.CancellationToken;

        // Phase 1: provision upstream → downstream → queue. Publish to upstream and verify the message
        // reaches the queue through the exchange-to-exchange binding chain.
        await using var phase1ServiceProvider = BuildServiceProvider(
            builder =>
            {
                builder.Exchange(upstream, ExchangeType.Direct);
                builder.Exchange(downstream, ExchangeType.Direct);
                builder.Queue(queue);
                builder.ExchangeBinding(upstream, downstream, "route");
                builder.QueueBinding(downstream, queue, "route");
            }
        );
        var phase1HostedServices = phase1ServiceProvider.GetServices<IHostedService>().ToArray();

        try
        {
            await StartAllAsync(phase1HostedServices, cancellationToken);
            await PublishAsync(upstream, "route", "{\"v\":1}", cancellationToken);

            await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
            var connectionFactory = new ConnectionFactory { Uri = new Uri(_container.GetConnectionString()) };
            await using var connection = await connectionFactory.CreateConnectionAsync(cancellationToken);
            await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);
            (await channel.MessageCountAsync(queue, cancellationToken)).Should().BeGreaterThan(0);
            await DrainAsync(channel, queue, cancellationToken);
        }
        finally
        {
            // Phase 2: flip downstream to Delete. Keep both bindings Active so the skip-set must skip them
            // (the binding loop skips any binding naming a Delete-mode exchange on either end). The provisioner
            // deletes downstream, and the broker cascade-removes both upstream→downstream and downstream→queue.
            await StopAllAsync(phase1HostedServices);
        }

        await using var phase2ServiceProvider = BuildServiceProvider(
            builder =>
            {
                builder.Exchange(upstream, ExchangeType.Direct);
                builder.Exchange(
                    downstream,
                    ExchangeType.Direct,
                    exchange => exchange.WithDeclareMode(RabbitMqDeclareMode.Delete)
                );
                builder.Queue(queue);
                // Both bindings are kept Active — the skip-set must skip them because downstream is Delete.
                builder.ExchangeBinding(upstream, downstream, "route");
                builder.QueueBinding(downstream, queue, "route");
            }
        );
        var phase2HostedServices = phase2ServiceProvider.GetServices<IHostedService>().ToArray();

        try
        {
            await StartAllAsync(phase2HostedServices, cancellationToken);

            // The downstream exchange should be absent on the broker.
            await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
            var connectionFactory = new ConnectionFactory { Uri = new Uri(_container.GetConnectionString()) };
            await using var connection = await connectionFactory.CreateConnectionAsync(cancellationToken);

            // AssertExchangeAbsentAsync closes the channel with a 404, so use a dedicated channel for it.
            await using (var absentChannel = await connection.CreateChannelAsync(cancellationToken: cancellationToken))
            {
                await AssertExchangeAbsentAsync(absentChannel, downstream, cancellationToken);
            }

            // Publishing to upstream no longer reaches the queue: downstream is gone, and the broker
            // cascade-removed the upstream→downstream binding.
            await PublishAsync(upstream, "route", "{\"v\":2}", cancellationToken);
            await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
            await using var verifyChannel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);
            (await verifyChannel.MessageCountAsync(queue, cancellationToken)).Should().Be(0);

            await StopAllAsync(phase2HostedServices);

            // Phase 3: idempotent re-delete — re-provision with downstream still in Delete mode. The exchange
            // is already absent, so ExchangeDeleteAsync raises a 404 which the provisioner treats as success.
            await StartAllAsync(phase2HostedServices, cancellationToken);
            await using (var reDeleteChannel =
                         await connection.CreateChannelAsync(cancellationToken: cancellationToken))
            {
                await AssertExchangeAbsentAsync(reDeleteChannel, downstream, cancellationToken);
            }
        }
        finally
        {
            await StopAllAsync(phase2HostedServices);
        }
    }

    [Fact]
    public async Task DeleteExchange_CascadesToSourceExchangeBindings()
    {
        // Deleting the source exchange removes its outgoing binding (source→destination). The destination
        // exchange and its downstream binding (destination→queue) are unaffected, so publishing directly to
        // the destination still reaches the queue.
        const string source = "policy-xsrc-source";
        const string destination = "policy-xsrc-destination";
        const string queue = "policy-xsrc-queue";
        var cancellationToken = TestContext.Current.CancellationToken;

        // Phase 1: source → destination → queue. Verify routing works end-to-end.
        await using var phase1ServiceProvider = BuildServiceProvider(
            builder =>
            {
                builder.Exchange(source, ExchangeType.Direct);
                builder.Exchange(destination, ExchangeType.Direct);
                builder.Queue(queue);
                builder.ExchangeBinding(source, destination, "route");
                builder.QueueBinding(destination, queue, "route");
            }
        );
        var phase1HostedServices = phase1ServiceProvider.GetServices<IHostedService>().ToArray();

        try
        {
            await StartAllAsync(phase1HostedServices, cancellationToken);
            await PublishAsync(source, "route", "{\"v\":1}", cancellationToken);

            await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
            var connectionFactory = new ConnectionFactory { Uri = new Uri(_container.GetConnectionString()) };
            await using var connection = await connectionFactory.CreateConnectionAsync(cancellationToken);
            await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);
            (await channel.MessageCountAsync(queue, cancellationToken)).Should().BeGreaterThan(0);
            await DrainAsync(channel, queue, cancellationToken);
        }
        finally
        {
            await StopAllAsync(phase1HostedServices);
        }

        // Phase 2: flip source to Delete. Keep both bindings Active so the skip-set must skip them.
        await using var phase2ServiceProvider = BuildServiceProvider(
            builder =>
            {
                builder.Exchange(
                    source,
                    ExchangeType.Direct,
                    exchange => exchange.WithDeclareMode(RabbitMqDeclareMode.Delete)
                );
                builder.Exchange(destination, ExchangeType.Direct);
                builder.Queue(queue);
                builder.ExchangeBinding(source, destination, "route");
                builder.QueueBinding(destination, queue, "route");
            }
        );
        var phase2HostedServices = phase2ServiceProvider.GetServices<IHostedService>().ToArray();

        try
        {
            await StartAllAsync(phase2HostedServices, cancellationToken);

            await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
            var connectionFactory = new ConnectionFactory { Uri = new Uri(_container.GetConnectionString()) };
            await using var connection = await connectionFactory.CreateConnectionAsync(cancellationToken);

            // The source exchange should be absent — use a dedicated channel (the 404 closes it).
            await using (var absentChannel = await connection.CreateChannelAsync(cancellationToken: cancellationToken))
            {
                await AssertExchangeAbsentAsync(absentChannel, source, cancellationToken);
            }

            // The destination exchange should still exist.
            await using var verifyChannel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);
            await verifyChannel.ExchangeDeclarePassiveAsync(destination, cancellationToken);

            // Publishing directly to the destination still reaches the queue — the destination→queue binding
            // was not removed because only the source was deleted.
            await PublishAsync(destination, "route", "{\"v\":2}", cancellationToken);
            await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
            (await verifyChannel.MessageCountAsync(queue, cancellationToken)).Should().BeGreaterThan(0);
        }
        finally
        {
            await StopAllAsync(phase2HostedServices);
        }
    }

    [Fact]
    public async Task DeleteExchangeBinding_UnbindsExistingExchangeBinding()
    {
        // Flipping an exchange binding to Delete removes it from the broker, breaking the routing chain.
        const string source = "policy-xunbind-source";
        const string destination = "policy-xunbind-destination";
        const string queue = "policy-xunbind-queue";
        var cancellationToken = TestContext.Current.CancellationToken;

        // Phase 1: source → destination → queue. Verify routing works.
        await using var phase1ServiceProvider = BuildServiceProvider(
            builder =>
            {
                builder.Exchange(source, ExchangeType.Direct);
                builder.Exchange(destination, ExchangeType.Direct);
                builder.Queue(queue);
                builder.ExchangeBinding(source, destination, "route");
                builder.QueueBinding(destination, queue, "route");
            }
        );
        var phase1HostedServices = phase1ServiceProvider.GetServices<IHostedService>().ToArray();

        try
        {
            await StartAllAsync(phase1HostedServices, cancellationToken);
            await PublishAsync(source, "route", "{\"v\":1}", cancellationToken);

            await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
            var connectionFactory = new ConnectionFactory { Uri = new Uri(_container.GetConnectionString()) };
            await using var connection = await connectionFactory.CreateConnectionAsync(cancellationToken);
            await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);
            (await channel.MessageCountAsync(queue, cancellationToken)).Should().BeGreaterThan(0);
            await DrainAsync(channel, queue, cancellationToken);

            await StopAllAsync(phase1HostedServices);
        }
        finally
        {
            await StopAllAsync(phase1HostedServices);
        }

        // Phase 2: flip the source→destination binding to Delete. The provisioner unbinds it.
        await using var phase2ServiceProvider = BuildServiceProvider(
            builder =>
            {
                builder.Exchange(source, ExchangeType.Direct);
                builder.Exchange(destination, ExchangeType.Direct);
                builder.Queue(queue);
                builder.ExchangeBinding(
                    source,
                    destination,
                    "route",
                    binding => binding.WithBindingMode(RabbitMqBindingMode.Delete)
                );
                builder.QueueBinding(destination, queue, "route");
            }
        );
        var phase2HostedServices = phase2ServiceProvider.GetServices<IHostedService>().ToArray();

        try
        {
            await StartAllAsync(phase2HostedServices, cancellationToken);

            // Publishing to source no longer reaches the queue: the source→destination binding was unbound.
            await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
            await PublishAsync(source, "route", "{\"v\":2}", cancellationToken);
            await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);

            var connectionFactory = new ConnectionFactory { Uri = new Uri(_container.GetConnectionString()) };
            await using var connection = await connectionFactory.CreateConnectionAsync(cancellationToken);
            await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);
            (await channel.MessageCountAsync(queue, cancellationToken)).Should().Be(0);

            // Publishing directly to destination still reaches the queue — the destination→queue binding is intact.
            await PublishAsync(destination, "route", "{\"v\":3}", cancellationToken);
            await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
            (await channel.MessageCountAsync(queue, cancellationToken)).Should().BeGreaterThan(0);
        }
        finally
        {
            await StopAllAsync(phase2HostedServices);
        }
    }

    [Fact]
    public async Task DeleteExchange_SucceedsWhenExchangeAlreadyAbsent()
    {
        // A Delete-mode exchange that was never declared on the broker should succeed — the 404 is treated as
        // success, making Delete idempotent across restarts.
        const string absentExchange = "policy-xabsent-exchange";
        var cancellationToken = TestContext.Current.CancellationToken;

        await using var serviceProvider = BuildServiceProvider(
            builder =>
            {
                builder.Exchange(
                    absentExchange,
                    ExchangeType.Direct,
                    exchange => exchange.WithDeclareMode(RabbitMqDeclareMode.Delete)
                );
            }
        );
        var hostedServices = serviceProvider.GetServices<IHostedService>().ToArray();

        var act = async () => await StartAllAsync(hostedServices, cancellationToken);

        await act.Should().NotThrowAsync<Exception>();
    }

    [Fact]
    public async Task HeadersBinding_AllWithXMatchesXPrefixedHeader_PlainAllIgnoresIt()
    {
        // This pins the RabbitMQ behavior the topology compiler guards against. A headers binding with
        // all-with-x matches on x-prefixed headers; the same x-prefixed header with plain all is excluded from
        // matching, leaving zero effective predicates. Plain all therefore matches every message.
        const string exchange = "policy-xheaders-exchange";
        const string allWithXQueue = "policy-xheaders-all-with-x";
        const string allQueue = "policy-xheaders-all";
        var cancellationToken = TestContext.Current.CancellationToken;

        var connectionFactory = new ConnectionFactory { Uri = new Uri(_container.GetConnectionString()) };
        await using var connection = await connectionFactory.CreateConnectionAsync(cancellationToken);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);

        await channel.ExchangeDeclareAsync(
            exchange,
            ExchangeType.Headers,
            durable: true,
            autoDelete: false,
            cancellationToken: cancellationToken
        );
        await channel.QueueDeclareAsync(
            allWithXQueue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: new Dictionary<string, object?>(0, StringComparer.Ordinal),
            cancellationToken: cancellationToken
        );
        await channel.QueueDeclareAsync(
            allQueue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: new Dictionary<string, object?>(0, StringComparer.Ordinal),
            cancellationToken: cancellationToken
        );
        await channel.QueueBindAsync(
            allWithXQueue,
            exchange,
            "",
            new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["x-match"] = "all-with-x",
                ["x-tenant"] = "acme"
            },
            false,
            cancellationToken
        );
        await channel.QueueBindAsync(
            allQueue,
            exchange,
            "",
            new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["x-match"] = "all",
                ["x-tenant"] = "acme"
            },
            false,
            cancellationToken
        );

        await PublishWithHeadersAsync(channel, exchange, "x-tenant", "acme", cancellationToken);
        await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
        (await channel.MessageCountAsync(allWithXQueue, cancellationToken)).Should().BeGreaterThan(0);
        (await channel.MessageCountAsync(allQueue, cancellationToken)).Should().BeGreaterThan(0);
        await DrainAsync(channel, allWithXQueue, cancellationToken);
        await DrainAsync(channel, allQueue, cancellationToken);

        await PublishWithHeadersAsync(channel, exchange, "x-tenant", "other", cancellationToken);
        await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
        (await channel.MessageCountAsync(allWithXQueue, cancellationToken)).Should().Be(0);
        (await channel.MessageCountAsync(allQueue, cancellationToken)).Should().BeGreaterThan(0);
    }

    private ServiceProvider BuildServiceProvider(Action<RabbitMqTopologyBuilder> configure)
    {
        var services = new ServiceCollection();
        services
           .AddTestCloudEvents()
           .AddRabbitMqTopology(
                builder =>
                {
                    builder.UseConnectionFactory(
                        _ => new ConnectionFactory { Uri = new Uri(_container.GetConnectionString()) }
                    );
                    configure(builder);
                }
            );
        return services.BuildServiceProvider();
    }

    private static async Task StartAllAsync(IHostedService[] hostedServices, CancellationToken cancellationToken)
    {
        foreach (var hostedService in hostedServices)
        {
            await hostedService.StartAsync(cancellationToken);
        }
    }

    private static async Task StopAllAsync(IHostedService[] hostedServices)
    {
        foreach (var hostedService in hostedServices.Reverse())
        {
            await hostedService.StopAsync(CancellationToken.None);
        }
    }

    private async Task PublishAsync(
        string exchange,
        string routingKey,
        string body,
        CancellationToken cancellationToken
    )
    {
        var factory = new ConnectionFactory { Uri = new Uri(_container.GetConnectionString()) };
        await using var connection = await factory.CreateConnectionAsync(cancellationToken);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);
        await channel.BasicPublishAsync(
            exchange,
            routingKey,
            false,
            body: Encoding.UTF8.GetBytes(body).AsMemory(),
            cancellationToken
        );
    }

    private static async Task PublishWithHeadersAsync(
        IChannel channel,
        string exchange,
        string headerName,
        string headerValue,
        CancellationToken cancellationToken
    )
    {
        var properties = new BasicProperties
        {
            Headers = new Dictionary<string, object?> { [headerName] = Encoding.UTF8.GetBytes(headerValue) }
        };
        var body = Encoding.UTF8.GetBytes("{}");
        await channel.BasicPublishAsync(
            exchange,
            "",
            true,
            properties,
            body.AsMemory(),
            cancellationToken
        );
    }

    private static async Task DrainAsync(IChannel channel, string queue, CancellationToken cancellationToken)
    {
        while (await channel.BasicGetAsync(queue, true, cancellationToken) is not null)
        {
            // Drain all messages.
        }
    }

    private static async Task AssertExchangeAbsentAsync(
        IChannel channel,
        string exchangeName,
        CancellationToken cancellationToken
    )
    {
        try
        {
            await channel.ExchangeDeclarePassiveAsync(exchangeName, cancellationToken);
            throw new InvalidOperationException($"Exchange '{exchangeName}' should have been absent on the broker.");
        }
        catch (Exception ex) when (IsNotFound(ex))
        {
            // The exchange is absent — expected.
        }
    }

    private static bool IsNotFound(Exception exception)
    {
        return exception is OperationInterruptedException { ShutdownReason: { ReplyCode: 404 } };
    }
}
