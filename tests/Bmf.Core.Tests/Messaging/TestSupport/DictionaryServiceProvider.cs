using System;
using System.Collections.Generic;

namespace Bmf.Core.Tests.Messaging.TestSupport;

public sealed class DictionaryServiceProvider : IServiceProvider
{
    private readonly Dictionary<Type, object> _services = new ();

    public DictionaryServiceProvider Add<TService>(TService service)
        where TService : notnull
    {
        _services[typeof(TService)] = service;
        return this;
    }

    public object? GetService(Type serviceType)
    {
        return _services.TryGetValue(serviceType, out var service) ? service : null;
    }
}
