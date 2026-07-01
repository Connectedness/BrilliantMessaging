using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Transport.Nats.Tests.TestSupport;
using FluentAssertions;
using NATS.Client.Core;
using Xunit;

namespace BrilliantMessaging.Transport.Nats.Tests.Integration;

[Collection<NatsCollection>]
public sealed class NatsConnectionProviderIntegrationTests : IAsyncLifetime
{
    private readonly NatsFixture _fixture;

    public NatsConnectionProviderIntegrationTests(NatsFixture fixture)
    {
        _fixture = fixture;
    }

    public async ValueTask InitializeAsync()
    {
        await _fixture.ResetAsync(TestContext.Current.CancellationToken);
    }

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }

    [Fact]
    public async Task GetJetStreamAsync_ReturnsCachedContextOnSubsequentCalls()
    {
        var url = _fixture.ConnectionString;
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
