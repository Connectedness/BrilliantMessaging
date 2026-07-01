using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NATS.Client.Core;
using NATS.Client.JetStream;
using Testcontainers.Nats;
using Xunit;

namespace BrilliantMessaging.Transport.Nats.Tests.TestSupport;

public sealed class NatsFixture : IAsyncLifetime
{
    public NatsFixture()
    {
        Container = new NatsBuilder(DockerImages.Nats)
           .WithCommand("-js")
           .Build();
    }

    public NatsContainer Container { get; }

    public string ConnectionString => Container.GetConnectionString();

    public async ValueTask InitializeAsync()
    {
        await Container.StartAsync();
    }

    public ValueTask DisposeAsync()
    {
        return Container.DisposeAsync();
    }

    public async Task ResetAsync(CancellationToken cancellationToken)
    {
        await using NatsConnection connection = new (new NatsOpts { Url = ConnectionString });
        NatsJSContext jetStream = new (connection);
        List<string> streamNames = [];

        await foreach (var streamName in jetStream
                          .ListStreamNamesAsync(cancellationToken: cancellationToken)
                          .ConfigureAwait(false))
        {
            streamNames.Add(streamName);
        }

        foreach (var streamName in streamNames)
        {
            await jetStream.DeleteStreamAsync(streamName, cancellationToken).ConfigureAwait(false);
        }
    }
}
