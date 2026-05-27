using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Usf.Transport.RabbitMq.Tests.TestSupport;

public sealed class TestRabbitMqConnection
{
    private readonly Queue<IChannel> _channels = new ();
    private readonly IConnection _connection;

    public TestRabbitMqConnection()
    {
        _connection = RabbitMqDispatchProxy<IConnection>.Create(HandleInvoke);
    }

    public ushort ChannelMax { get; set; }

    public ShutdownEventArgs? CloseReason { get; private set; }

    public int DisposeAsyncCallCount { get; private set; }

    public int DisposeCallCount { get; private set; }

    public bool IsOpen { get; private set; } = true;

    public IConnection Object => _connection;

    public void EnqueueChannel(IChannel channel)
    {
        _channels.Enqueue(channel ?? throw new ArgumentNullException(nameof(channel)));
    }

    private object? HandleInvoke(MethodInfo targetMethod, object?[]? arguments)
    {
        switch (targetMethod.Name)
        {
            case "get_ChannelMax":
                return ChannelMax;
            case "get_IsOpen":
                return IsOpen;
            case "get_CloseReason":
                return CloseReason;
            case "CreateChannelAsync":
                return Task.FromResult(_channels.Dequeue());
            case "DisposeAsync":
                DisposeAsyncCallCount++;
                IsOpen = false;
                return default(ValueTask);
            case "Dispose":
                DisposeCallCount++;
                IsOpen = false;
                return null;
        }

        if (targetMethod.Name.StartsWith("add_", StringComparison.Ordinal) ||
            targetMethod.Name.StartsWith("remove_", StringComparison.Ordinal))
        {
            return null;
        }

        return RabbitMqDispatchProxyDefaults.GetDefaultValue(targetMethod.ReturnType);
    }
}
