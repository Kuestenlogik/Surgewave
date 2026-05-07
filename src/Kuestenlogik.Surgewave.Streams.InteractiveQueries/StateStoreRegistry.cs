using System.Collections.Concurrent;

namespace Kuestenlogik.Surgewave.Streams.InteractiveQueries;

/// <summary>
/// Thread-safe registry of state stores for Interactive Queries.
/// Determines each store's <see cref="StateStoreType"/> by inspecting
/// the generic interfaces it implements.
/// </summary>
public sealed class StateStoreRegistry : IStateStoreRegistry
{
    private readonly ConcurrentDictionary<string, IStateStore> _stores = new(StringComparer.Ordinal);

    /// <inheritdoc/>
    public void Register(string name, IStateStore store)
    {
        ArgumentNullException.ThrowIfNull(store);
        _stores[name] = store;
    }

    /// <inheritdoc/>
    public void Unregister(string name)
    {
        _stores.TryRemove(name, out _);
    }

    /// <inheritdoc/>
    public IStateStore? GetStore(string name)
    {
        _stores.TryGetValue(name, out var store);
        return store;
    }

    /// <inheritdoc/>
    public StateStoreInfo? GetStoreInfo(string name)
    {
        if (!_stores.TryGetValue(name, out var store))
            return null;

        return BuildInfo(name, store);
    }

    /// <inheritdoc/>
    public IReadOnlyList<StateStoreInfo> GetAllStores()
    {
        var result = new List<StateStoreInfo>(_stores.Count);
        foreach (var (name, store) in _stores)
            result.Add(BuildInfo(name, store));
        return result;
    }

    private static StateStoreInfo BuildInfo(string name, IStateStore store)
    {
        var storeType = DetectStoreType(store);
        var entryCount = GetApproximateEntryCount(store);
        return new StateStoreInfo(name, storeType, store.Persistent, entryCount);
    }

    private static StateStoreType DetectStoreType(IStateStore store)
    {
        var type = store.GetType();
        foreach (var iface in type.GetInterfaces())
        {
            if (!iface.IsGenericType)
                continue;

            var def = iface.GetGenericTypeDefinition();

            if (def == typeof(IReadOnlyKeyValueStore<,>))
                return StateStoreType.KeyValue;

            if (def == typeof(IWindowStore<,>))
                return StateStoreType.Window;

            if (def == typeof(ISessionStore<,>))
                return StateStoreType.Session;
        }

        return StateStoreType.Unknown;
    }

    private static long GetApproximateEntryCount(IStateStore store)
    {
        var prop = store.GetType().GetProperty("ApproximateNumEntries");
        if (prop?.GetValue(store) is long count)
            return count;
        return 0;
    }
}
