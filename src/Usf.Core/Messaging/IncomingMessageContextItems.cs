using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace Usf.Core.Messaging;

/// <summary>
/// A strongly-typed, key-addressable bag of side-band values that flow alongside a message
/// through the inbound pipeline. An inspector can pre-seed an instance with data later middleware
/// or handlers depend on (for example a parsed CloudEvents envelope, a decryption key, or a schema
/// id), and the runtime adopts that instance as the <see cref="IncomingMessageContext.Items" /> of
/// the context it builds — no copy occurs. Instances are not thread-safe and are intended to be used
/// by the single pipeline that owns the message.
/// </summary>
public sealed class IncomingMessageContextItems
{
    private readonly Dictionary<object, object?> _items = new ();

    /// <summary>
    /// Stores <paramref name="value" /> under <paramref name="key" />, replacing any existing value.
    /// </summary>
    /// <typeparam name="T">The type of the stored value, carried by the key.</typeparam>
    /// <param name="key">The strongly-typed key identifying the item.</param>
    /// <param name="value">The value to store.</param>
    public void SetItem<T>(MessageContextKey<T> key, T value)
    {
        _items[key] = value;
    }

    /// <summary>
    /// Attempts to retrieve the value stored under <paramref name="key" />.
    /// </summary>
    /// <typeparam name="T">The type of the stored value, carried by the key.</typeparam>
    /// <param name="key">The strongly-typed key identifying the item.</param>
    /// <param name="value">
    /// When this method returns <see langword="true" />, the stored value; otherwise the default of
    /// <typeparamref name="T" />.
    /// </param>
    /// <returns><see langword="true" /> if a value of the expected type was found; otherwise <see langword="false" />.</returns>
    public bool TryGetItem<T>(MessageContextKey<T> key, [NotNullWhen(true)] out T? value)
    {
        if (_items.TryGetValue(key, out var item) && item is T typedValue)
        {
            value = typedValue;
            return true;
        }

        value = default;
        return false;
    }

    /// <summary>
    /// Retrieves the value stored under <paramref name="key" />, throwing when it is absent.
    /// </summary>
    /// <typeparam name="T">The type of the stored value, carried by the key.</typeparam>
    /// <param name="key">The strongly-typed key identifying the item.</param>
    /// <returns>The stored value.</returns>
    /// <exception cref="InvalidOperationException">No value is stored under <paramref name="key" />.</exception>
    public T GetRequiredItem<T>(MessageContextKey<T> key) =>
        TryGetItem(key, out var value) ?
            value :
            throw new InvalidOperationException($"Message context item '{key.Name}' is not set.");

    /// <summary>
    /// Removes the value stored under <paramref name="key" />, if present.
    /// </summary>
    /// <typeparam name="T">The type of the stored value, carried by the key.</typeparam>
    /// <param name="key">The strongly-typed key identifying the item.</param>
    /// <returns><see langword="true" /> if an item was removed; otherwise <see langword="false" />.</returns>
    public bool RemoveItem<T>(MessageContextKey<T> key)
    {
        return _items.Remove(key);
    }
}
