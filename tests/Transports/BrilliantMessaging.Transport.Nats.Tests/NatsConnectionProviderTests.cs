using System;
using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging;
using FluentAssertions;
using NATS.Client.Core;
using Testcontainers.Nats;
using Xunit;

namespace BrilliantMessaging.Transport.Nats.Tests;

public sealed class NatsConnectionProviderTests
{
    [Fact]
    public void Constructor_NullOptionsFactory_ThrowsArgumentNullException()
    {
        var act = () => new NatsConnectionProvider(null!);

        act.Should().Throw<ArgumentNullException>().WithParameterName("createOptions");
    }

    [Fact]
    public async Task GetJetStreamAsync_WhenOptionsFactoryReturnsNull_ThrowsTopologyValidationException()
    {
        await using NatsConnectionProvider provider = new (_ => Task.FromResult<NatsOpts>(null!));

        var act = () => provider.GetJetStreamAsync(TestContext.Current.CancellationToken);

        var exception = await act.Should().ThrowAsync<TopologyValidationException>();
        exception.Which.ValidationErrors.Should()
           .Contain(error => error.Contains("null", StringComparison.Ordinal));
    }

    [Fact]
    public async Task GetJetStreamAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        NatsConnectionProvider provider = new (_ => Task.FromResult(new NatsOpts()));
        await provider.DisposeAsync();

        var act = () => provider.GetJetStreamAsync(TestContext.Current.CancellationToken);

        await act.Should().ThrowAsync<ObjectDisposedException>();
    }

    [Fact]
    public async Task DisposeAsync_CalledTwice_DoesNotThrow()
    {
        NatsConnectionProvider provider = new (_ => Task.FromResult(new NatsOpts()));

        await provider.DisposeAsync();
        var act = async () => await provider.DisposeAsync();

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task GetJetStreamAsync_WhenConnectionFails_Rethrows()
    {
        // Point at a closed port so ConnectAsync fails fast, exercising the connect-failure clean-up path.
        NatsOpts options = new ()
        {
            Url = "nats://127.0.0.1:14344",
            ConnectTimeout = TimeSpan.FromMilliseconds(250),
            MaxReconnectRetry = 0
        };
        await using NatsConnectionProvider provider = new (_ => Task.FromResult(options));

        var act = () => provider.GetJetStreamAsync(TestContext.Current.CancellationToken);

        await act.Should().ThrowAsync<NatsException>();
    }

    [Fact]
    public async Task GetJetStreamAsync_ReturnsCachedContextOnSubsequentCalls()
    {
        await using var container = new NatsBuilder("nats:2.11-alpine")
           .WithCommand("-js")
           .Build();
        await container.StartAsync(TestContext.Current.CancellationToken);

        var url = container.GetConnectionString();
        var invocations = 0;
        await using NatsConnectionProvider provider = new (
            _ =>
            {
                Interlocked.Increment(ref invocations);
                return Task.FromResult(new NatsOpts { Url = url });
            }
        );

        var first = await provider.GetJetStreamAsync(TestContext.Current.CancellationToken);
        var second = await provider.GetJetStreamAsync(TestContext.Current.CancellationToken);

        second.Should().BeSameAs(first);
        invocations.Should().Be(1);
    }
}
