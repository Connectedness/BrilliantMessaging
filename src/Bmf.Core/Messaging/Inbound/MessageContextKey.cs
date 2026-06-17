namespace Bmf.Core.Messaging.Inbound;

/// <summary>
/// A strongly typed key for a value of type <typeparamref name="T" /> stored in
/// <see cref="IncomingMessageContextItems" />. The type parameter ties the key to the value type so reads and
/// writes are type-safe.
/// </summary>
/// <typeparam name="T">The type of value the key identifies.</typeparam>
/// <param name="Name">The unique name of the key.</param>
public sealed record MessageContextKey<T>(string Name);
