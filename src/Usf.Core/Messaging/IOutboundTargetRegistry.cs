namespace Usf.Core.Messaging;

public interface IOutboundTargetRegistry
{
    OutboundTarget GetRequiredTarget(string name);

    OutboundTarget<T> GetRequiredTarget<T>(string name);

    bool TryGetTarget(string name, out OutboundTarget? target);
}
