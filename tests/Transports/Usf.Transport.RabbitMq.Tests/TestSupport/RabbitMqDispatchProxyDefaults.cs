using System;
using System.Reflection;
using System.Threading.Tasks;

namespace Usf.Transport.RabbitMq.Tests.TestSupport;

public static class RabbitMqDispatchProxyDefaults
{
    private static readonly MethodInfo TaskFromResultMethod = typeof(Task).GetMethod(nameof(Task.FromResult))!;

    public static object? GetDefaultValue(Type type)
    {
        if (type == typeof(void))
        {
            return null;
        }

        if (type == typeof(Task))
        {
            return Task.CompletedTask;
        }

        if (type == typeof(ValueTask))
        {
            return default(ValueTask);
        }

        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Task<>))
        {
            var resultType = type.GenericTypeArguments[0];
            return TaskFromResultMethod.MakeGenericMethod(resultType).Invoke(null, [GetDefaultResult(resultType)]);
        }

        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(ValueTask<>))
        {
            var resultType = type.GenericTypeArguments[0];
            return Activator.CreateInstance(type, GetDefaultResult(resultType));
        }

        return GetDefaultResult(type);
    }

    private static object? GetDefaultResult(Type type)
    {
        return type.IsValueType ? Activator.CreateInstance(type) : null;
    }
}
