using System;
using System.Threading;
using System.Threading.Tasks;
using Usf.Core.Messaging;

namespace Usf.Core.Tests.Messaging.TestSupport;

public sealed class ThrowingTarget<TMessage> : OutboundTarget<TMessage>
{
    private readonly Exception _exception;

    public ThrowingTarget(string name, IMessageSerializer serializer, Exception exception)
        : base(name, "test", serializer)
    {
        _exception = exception ?? throw new ArgumentNullException(nameof(exception));
    }

    public override Task PublishSerializedAsync(
        SerializedMessage message,
        CancellationToken cancellationToken = default
    )
    {
        return Task.FromException(_exception);
    }
}
