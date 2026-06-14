using System;
using System.Collections.Generic;

namespace Usf.Core.Messaging;

public sealed class IncomingMessageContextItems
{
    private Dictionary<object, object?>? _items;

    public void SetItem<T>(MessageContextKey<T> key, T value)
    {
        _items ??= new Dictionary<object, object?>();
        _items[key] = value;
    }

    public bool TryGetItem<T>(MessageContextKey<T> key, out T? value)
    {
        if (_items is not null &&
            _items.TryGetValue(key, out var item) &&
            item is T typedValue)
        {
            value = typedValue;
            return true;
        }

        value = default;
        return false;
    }

    public T GetRequiredItem<T>(MessageContextKey<T> key)
    {
        if (TryGetItem(key, out var value))
        {
            return value!;
        }

        throw new InvalidOperationException($"Message context item '{key.Name}' is not set.");
    }

    public bool RemoveItem<T>(MessageContextKey<T> key)
    {
        return _items is not null && _items.Remove(key);
    }
}
