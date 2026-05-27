namespace Usf.Transport.RabbitMq;

public enum RabbitMqChannelPoolingMode
{
    PerTarget = 0,

    /// <summary>
    /// Uses a single connection-level channel pool for all targets. This reduces channel count, but per-target
    /// publish ordering is not guaranteed.
    /// </summary>
    Shared = 1
}
