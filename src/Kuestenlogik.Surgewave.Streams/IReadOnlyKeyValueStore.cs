namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// Read-only key-value state store interface for interactive queries.
/// </summary>
/// <typeparam name="TKey">The key type.</typeparam>
/// <typeparam name="TValue">The value type.</typeparam>
public interface IReadOnlyKeyValueStore<TKey, TValue> : IStateStore
{
    /// <summary>Gets the value associated with the given key, or null if not found.</summary>
    /// <param name="key">The key to look up.</param>
    /// <returns>The value, or null if the key does not exist.</returns>
    TValue? Get(TKey key);

    /// <summary>Returns all entries in the specified key range (inclusive).</summary>
    /// <param name="from">The start key (inclusive).</param>
    /// <param name="to">The end key (inclusive).</param>
    /// <returns>An enumerable of key-value pairs in the range.</returns>
    IEnumerable<KeyValue<TKey, TValue>> Range(TKey from, TKey to);

    /// <summary>Returns all entries in the store.</summary>
    /// <returns>An enumerable of all key-value pairs.</returns>
    IEnumerable<KeyValue<TKey, TValue>> All();

    /// <summary>
    /// Returns all entries in the specified key range in descending key order
    /// (KIP-985). The default implementation buffers the forward range and reverses
    /// it; backends with native reverse iteration (RocksDB, sorted file stores)
    /// should override for O(1) memory usage.
    /// </summary>
    /// <param name="from">The start key (inclusive). The semantically larger key — the iteration begins here.</param>
    /// <param name="to">The end key (inclusive). The semantically smaller key — the iteration ends here.</param>
    IEnumerable<KeyValue<TKey, TValue>> ReverseRange(TKey from, TKey to)
        => Range(to, from).Reverse();

    /// <summary>
    /// Returns all entries in the store in descending key order (KIP-985). The
    /// default implementation buffers <see cref="All"/> and reverses it; backends
    /// with native reverse iteration should override.
    /// </summary>
    IEnumerable<KeyValue<TKey, TValue>> ReverseAll()
        => All().Reverse();

    /// <summary>Gets the approximate number of entries in the store.</summary>
    long ApproximateNumEntries { get; }
}
