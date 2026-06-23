using System;
using Microsoft.Extensions.Logging;

namespace BrilliantMessaging.Transport.RabbitMq.Tests.TestSupport;

public sealed class RecordingLoggerFactory : ILoggerFactory
{
    private readonly ILoggerProvider _provider;

    public RecordingLoggerFactory(ILoggerProvider provider)
    {
        _provider = provider ?? throw new ArgumentNullException(nameof(provider));
    }

    public void AddProvider(ILoggerProvider provider)
    {
        throw new NotSupportedException();
    }

    public ILogger CreateLogger(string categoryName)
    {
        return _provider.CreateLogger(categoryName);
    }

    public void Dispose()
    {
        _provider.Dispose();
    }
}
