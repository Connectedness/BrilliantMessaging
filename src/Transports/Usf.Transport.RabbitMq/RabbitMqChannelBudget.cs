using System;

namespace Usf.Transport.RabbitMq;

public static class RabbitMqChannelBudget
{
    public static (int WorstCaseChannelCount, string Description) Calculate(
        RabbitMqPublishingConfiguration configuration
    )
    {
        if (configuration is null)
        {
            throw new ArgumentNullException(nameof(configuration));
        }

        if (configuration.Routes.Count == 0)
        {
            return (0, "no publish routes configured");
        }

        return configuration.ChannelPoolingMode switch
        {
            RabbitMqChannelPoolingMode.PerTarget => (
                checked(configuration.Routes.Count * configuration.MaxChannelsPerTarget),
                $"PerTarget mode, {configuration.Routes.Count} targets × max {configuration.MaxChannelsPerTarget}"
            ),
            RabbitMqChannelPoolingMode.Shared => (
                configuration.SharedChannelPoolSize,
                $"Shared mode, shared pool size {configuration.SharedChannelPoolSize}"
            ),
            _ => (0, "unknown pooling mode")
        };
    }
}
