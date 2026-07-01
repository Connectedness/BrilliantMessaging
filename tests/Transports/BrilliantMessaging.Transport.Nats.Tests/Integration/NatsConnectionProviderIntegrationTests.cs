using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using NATS.Client.Core;
using Testcontainers.Nats;
using Xunit;

namespace BrilliantMessaging.Transport.Nats.Tests.Integration;

public sealed class NatsConnectionProviderIntegrationTests
{
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
