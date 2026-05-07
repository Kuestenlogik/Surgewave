using Kuestenlogik.Surgewave.Streams;
using Kuestenlogik.Surgewave.Streams.InteractiveQueries;

namespace Kuestenlogik.Surgewave.Streams.Linq;

/// <summary>
/// LINQ extensions for querying Surgewave Streams state stores.
/// </summary>
public static class StateStoreQueryExtensions
{
    /// <summary>
    /// Returns all entries in a key-value state store as IQueryable for LINQ queries.
    /// </summary>
    public static IQueryable<KeyValue<TKey, TValue>> AsQueryable<TKey, TValue>(
        this IReadOnlyKeyValueStore<TKey, TValue> store)
    {
        return store.All().AsQueryable();
    }

    /// <summary>
    /// Returns entries in a key range as IQueryable for LINQ queries.
    /// </summary>
    public static IQueryable<KeyValue<TKey, TValue>> AsQueryable<TKey, TValue>(
        this IReadOnlyKeyValueStore<TKey, TValue> store, TKey from, TKey to)
    {
        return store.Range(from, to).AsQueryable();
    }

    /// <summary>
    /// Queries a named state store from the registry as IQueryable.
    /// </summary>
    public static IQueryable<KeyValue<TKey, TValue>> QueryStore<TKey, TValue>(
        this IStateStoreRegistry registry, string storeName)
    {
        var store = registry.GetStore(storeName)
            ?? throw new InvalidOperationException($"State store '{storeName}' not found.");

        if (store is not IReadOnlyKeyValueStore<TKey, TValue> kvStore)
            throw new InvalidOperationException(
                $"State store '{storeName}' is not a IReadOnlyKeyValueStore<{typeof(TKey).Name}, {typeof(TValue).Name}>.");

        return kvStore.AsQueryable();
    }
}
