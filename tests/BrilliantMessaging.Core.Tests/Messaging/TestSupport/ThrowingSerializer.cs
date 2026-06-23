using System;
using System.Threading;
using System.Threading.Tasks;
using BrilliantMessaging.Core.Messaging;
using BrilliantMessaging.Core.Messaging.Outbound;

namespace BrilliantMessaging.Core.Tests.Messaging.TestSupport;

public sealed class ThrowingSerializer : IMessageSerializer
{
    private readonly Exception _exception;

    public ThrowingSerializer(Exception exception)
    {
        _exception = exception;
    }

    public ValueTask<CloudEventEnvelope> SerializeAsync<T>(
        T message,
        in CloudEventMetadata metadata,
        string? type,
        string? dataSchema,
        CancellationToken cancellationToken = default
    )
    {
        throw _exception;
    }
}
