using System;
using System.Collections.Generic;
using Usf.Core.Messaging;
using Usf.Core.Messaging.Errors;

namespace Usf.Core.Tests.Messaging.TestSupport;

public sealed class EmptyOutboundTopology : IOutboundTopology
{
    public IReadOnlyCollection<OutboundTarget> Targets => [];

    public OutboundTarget GetRequiredTarget(Type messageType)
    {
        throw new OutboundTargetNotFoundException(messageType);
    }

    public OutboundTarget<T> GetRequiredTarget<T>()
    {
        throw new OutboundTargetNotFoundException(typeof(T));
    }

    public bool TryGetTarget(Type messageType, out OutboundTarget? target)
    {
        target = null;
        return false;
    }

    public OutboundTarget GetRequiredTarget(string name)
    {
        throw new OutboundTargetNotFoundException(name);
    }

    public OutboundTarget<T> GetRequiredTarget<T>(string name)
    {
        throw new OutboundTargetNotFoundException(name);
    }

    public bool TryGetTarget(string name, out OutboundTarget? target)
    {
        target = null;
        return false;
    }
}
