using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using Testcontainers.RabbitMq;
using Xunit;

namespace Usf.Transport.RabbitMq.Tests.TestSupport;

public sealed class RabbitMqFixture : IAsyncLifetime
{
    public RabbitMqFixture()
    {
        Container = new RabbitMqBuilder(DockerImages.RabbitMq)
           .WithPortBinding(GetAvailableTcpPort(), RabbitMqBuilder.RabbitMqPort)
           .Build();
    }

    public RabbitMqContainer Container { get; }

    public async ValueTask InitializeAsync()
    {
        await Container.StartAsync();
    }

    public ValueTask DisposeAsync()
    {
        return Container.DisposeAsync();
    }

    private static int GetAvailableTcpPort()
    {
        // Binding to port 0 asks the OS to select an available ephemeral port. The listener is
        // then released so Docker can bind that fixed port. This can theoretically lead to a race with another
        // process, which is unlikely for this test scenario.
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
