namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// Writable key-value state store interface.
/// Extends <see cref="IReadOnlyKeyValueStore{TKey,TValue}"/> with mutation operations.
/// </summary>
/// <typeparam name="TKey">The key type.</typeparam>
/// <typeparam name="TValue">The value type.</typeparam>
public interface IKeyValueStore<TKey, TValue> : IReadOnlyKeyValueStore<TKey, TValue>
{
    /// <summary>Inserts or updates the value associated with the given key.</summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The value.</param>
    void Put(TKey key, TValue value);

    /// <summary>Inserts the value only if the key does not already exist.</summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The value to insert.</param>
    /// <returns>The existing value if the key was present; otherwise null.</returns>
    TValue? PutIfAbsent(TKey key, TValue value);

    /// <summary>Inserts or updates multiple entries in a batch.</summary>
    /// <param name="entries">The key-value pairs to insert.</param>
    void PutAll(IEnumerable<KeyValue<TKey, TValue>> entries);

    /// <summary>Deletes the value associated with the given key.</summary>
    /// <param name="key">The key to delete.</param>
    /// <returns>The deleted value, or null if the key was not found.</returns>
    TValue? Delete(TKey key);
}
