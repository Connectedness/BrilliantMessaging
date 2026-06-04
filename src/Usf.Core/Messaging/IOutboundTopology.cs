using System;
using System.Collections.Generic;

namespace Usf.Core.Messaging;

public interface IOutboundTopology : IOutboundTargetRegistry
{
    IReadOnlyCollection<OutboundTarget> Targets { get; }

    OutboundTarget GetRequiredTarget(Type messageType);

    OutboundTarget<T> GetRequiredTarget<T>();

    bool TryGetTarget(Type messageType, out OutboundTarget? target);
}
