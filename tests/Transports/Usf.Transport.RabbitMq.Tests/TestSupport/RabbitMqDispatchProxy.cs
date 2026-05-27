using System;
using System.Reflection;

namespace Usf.Transport.RabbitMq.Tests.TestSupport;

public class RabbitMqDispatchProxy<T> : DispatchProxy where T : class
{
    public Func<MethodInfo, object?[]?, object?>? Handler { get; set; }

    public static T Create(Func<MethodInfo, object?[]?, object?> handler)
    {
        var proxy = Create<T, RabbitMqDispatchProxy<T>>();
        ((RabbitMqDispatchProxy<T>) (object) proxy).Handler = handler;
        return proxy;
    }

    protected override object? Invoke(MethodInfo? targetMethod, object?[]? arguments)
    {
        if (targetMethod is null)
        {
            throw new ArgumentNullException(nameof(targetMethod));
        }

        return Handler!(targetMethod, arguments);
    }
}
