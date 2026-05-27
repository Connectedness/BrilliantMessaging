using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Serialization;

namespace Usf.Transport.RabbitMq.Tests.TestSupport;

public sealed class TrackingTarget : Target<ValidationMessageA>, IAsyncDisposable, IDisposable
{
    private readonly IList<string> _events;

    public TrackingTarget(string name, IList<string> events)
        : base(name, "test", new Utf8JsonMessageSerializer())
    {
        _events = events ?? throw new ArgumentNullException(nameof(events));
    }

    public ValueTask DisposeAsync()
    {
        _events.Add(Name);
        return default;
    }

    public void Dispose()
    {
        _events.Add(Name);
    }

    protected override Task DispatchAsync(
        ValidationMessageA message,
        SerializedMessage serializedMessage,
        CancellationToken cancellationToken
    )
    {
        throw new NotSupportedException();
    }
}
