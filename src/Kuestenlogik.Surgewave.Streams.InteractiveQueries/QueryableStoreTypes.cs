namespace Kuestenlogik.Surgewave.Streams.InteractiveQueries;

/// <summary>
/// Factory for creating queryable store types for Interactive Queries.
/// </summary>
public static class QueryableStoreTypes
{
    /// <summary>
    /// Creates a read-only key-value store query type.
    /// </summary>
    public static IQueryableStoreType<ReadOnlyKeyValueStoreWrapper<TKey, TValue>> KeyValueStore<TKey, TValue>()
        where TKey : notnull
        => new KeyValueStoreType<TKey, TValue>();

    /// <summary>
    /// Creates a read-only window store query type.
    /// </summary>
    public static IQueryableStoreType<ReadOnlyWindowStoreWrapper<TKey, TValue>> WindowStore<TKey, TValue>()
        where TKey : notnull
        => new WindowStoreType<TKey, TValue>();

    /// <summary>
    /// Creates a read-only session store query type.
    /// </summary>
    public static IQueryableStoreType<ReadOnlySessionStoreWrapper<TKey, TValue>> SessionStore<TKey, TValue>()
        where TKey : notnull
        => new SessionStoreType<TKey, TValue>();

    private sealed class KeyValueStoreType<TKey, TValue> : IQueryableStoreType<ReadOnlyKeyValueStoreWrapper<TKey, TValue>>
        where TKey : notnull
    {
        public ReadOnlyKeyValueStoreWrapper<TKey, TValue>? Create(IStateStore store)
        {
            if (store is IKeyValueStore<TKey, TValue> kvStore)
                return new ReadOnlyKeyValueStoreWrapper<TKey, TValue>(kvStore);
            return null;
        }

        public bool Accepts(IStateStore store) => store is IKeyValueStore<TKey, TValue>;
    }

    private sealed class WindowStoreType<TKey, TValue> : IQueryableStoreType<ReadOnlyWindowStoreWrapper<TKey, TValue>>
        where TKey : notnull
    {
        public ReadOnlyWindowStoreWrapper<TKey, TValue>? Create(IStateStore store)
        {
            if (store is IWindowStore<TKey, TValue> windowStore)
                return new ReadOnlyWindowStoreWrapper<TKey, TValue>(windowStore);
            return null;
        }

        public bool Accepts(IStateStore store) => store is IWindowStore<TKey, TValue>;
    }

    private sealed class SessionStoreType<TKey, TValue> : IQueryableStoreType<ReadOnlySessionStoreWrapper<TKey, TValue>>
        where TKey : notnull
    {
        public ReadOnlySessionStoreWrapper<TKey, TValue>? Create(IStateStore store)
        {
            if (store is ISessionStore<TKey, TValue> sessionStore)
                return new ReadOnlySessionStoreWrapper<TKey, TValue>(sessionStore);
            return null;
        }

        public bool Accepts(IStateStore store) => store is ISessionStore<TKey, TValue>;
    }
}
