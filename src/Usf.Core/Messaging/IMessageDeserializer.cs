using System;
using System.Threading;
using System.Threading.Tasks;

namespace Usf.Core.Messaging;

public interface IMessageDeserializer
{
    ValueTask<object?> DeserializeAsync(
        IncomingMessageContext context,
        Type messageType,
        CancellationToken cancellationToken = default
    );
}
