using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using Testcontainers.Nats;
using Testcontainers.RabbitMq;
using Xunit;

namespace BrilliantMessaging.Transports.Integration.Tests.TestSupport;

public sealed class MultiTransportFixture : IAsyncLifetime
{
    public MultiTransportFixture()
    {
        RabbitMqContainer = new RabbitMqBuilder(DockerImages.RabbitMq)
           .WithPortBinding(GetAvailableTcpPort(), RabbitMqBuilder.RabbitMqPort)
           .Build();
        NatsContainer = new NatsBuilder(DockerImages.Nats)
           .WithCommand("-js")
           .Build();
    }

    public RabbitMqContainer RabbitMqContainer { get; }

    public NatsContainer NatsContainer { get; }

    public string RabbitMqConnectionString => RabbitMqContainer.GetConnectionString();

    public string NatsConnectionString => NatsContainer.GetConnectionString();

    public async ValueTask InitializeAsync()
    {
        await RabbitMqContainer.StartAsync();
        await NatsContainer.StartAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await NatsContainer.DisposeAsync();
        await RabbitMqContainer.DisposeAsync();
    }

    private static int GetAvailableTcpPort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();

        try
        {
            return ((IPEndPoint) listener.LocalEndpoint).Port;
        }
        finally
        {
            listener.Stop();
        }
    }
}
