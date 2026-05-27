using System;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Usf.Transport.RabbitMq.Tests.TestSupport;

public sealed class TestRabbitMqChannel
{
    private readonly IChannel _channel;
    private AsyncEventHandler<ShutdownEventArgs>? _channelShutdownAsync;
    private ShutdownEventArgs? _closeReason;

    public TestRabbitMqChannel()
    {
        _channel = RabbitMqDispatchProxy<IChannel>.Create(HandleInvoke);
    }

    public Func<ValueTask>? BasicPublishAsyncHandler { get; set; }

    public int BasicPublishCallCount { get; private set; }

    public ShutdownEventArgs? CloseReason => _closeReason;

    public int DisposeAsyncCallCount { get; private set; }

    public int DisposeCallCount { get; private set; }

    public bool IsOpen { get; private set; } = true;

    public IChannel Object => _channel;

    public void Close(ushort replyCode = 200, string replyText = "Closed")
    {
        IsOpen = false;
        _closeReason = CreateShutdownEvent(replyCode, replyText);
    }

    public async Task ShutdownAsync(ushort replyCode = 200, string replyText = "Closed")
    {
        Close(replyCode, replyText);

        if (_channelShutdownAsync is not null)
        {
            await _channelShutdownAsync(_channel, _closeReason!).ConfigureAwait(false);
        }
    }

    private object? HandleInvoke(MethodInfo targetMethod, object?[]? arguments)
    {
        switch (targetMethod.Name)
        {
            case "get_IsOpen":
                return IsOpen;
            case "get_IsClosed":
                return !IsOpen;
            case "get_CloseReason":
                return _closeReason;
            case "add_ChannelShutdownAsync":
                _channelShutdownAsync += (AsyncEventHandler<ShutdownEventArgs>) arguments![0]!;
                return null;
            case "remove_ChannelShutdownAsync":
                _channelShutdownAsync -= (AsyncEventHandler<ShutdownEventArgs>) arguments![0]!;
                return null;
            case "BasicPublishAsync":
                BasicPublishCallCount++;
                return BasicPublishAsyncHandler?.Invoke() ?? default(ValueTask);
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

    private static ShutdownEventArgs CreateShutdownEvent(ushort replyCode, string replyText)
    {
        return new ShutdownEventArgs(
            ShutdownInitiator.Library,
            replyCode,
            replyText,
            new object(),
            CancellationToken.None
        );
    }
}
