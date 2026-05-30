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

    public TestRabbitMqConnection(IList<string>? disposalEvents = null, string disposalEventName = "connection")
    {
        Object = RabbitMqDispatchProxy<IConnection>.Create(HandleInvoke);
        DisposalEvents = disposalEvents;
        DisposalEventName = disposalEventName;
    }

    public ushort ChannelMax { get; set; }

    public ShutdownEventArgs? CloseReason { get; private set; }

    public int DisposeAsyncCallCount { get; private set; }

    public int DisposeCallCount { get; private set; }

    public IList<CreateChannelOptions?> CreateChannelOptions { get; } = new List<CreateChannelOptions?>();

    public IList<string>? DisposalEvents { get; }

    public string DisposalEventName { get; }

    public bool IsOpen { get; private set; } = true;

    public IConnection Object { get; }

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
                CreateChannelOptions.Add((CreateChannelOptions?) arguments![0]);
                return Task.FromResult(_channels.Dequeue());
            case "DisposeAsync":
                DisposeAsyncCallCount++;
                IsOpen = false;
                DisposalEvents?.Add(DisposalEventName);
                return default(ValueTask);
            case "Dispose":
                DisposeCallCount++;
                IsOpen = false;
                DisposalEvents?.Add(DisposalEventName);
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
