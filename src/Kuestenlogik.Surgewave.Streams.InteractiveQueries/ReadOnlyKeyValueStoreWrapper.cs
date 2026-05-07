namespace Kuestenlogik.Surgewave.Streams.InteractiveQueries;

/// <summary>
/// Read-only wrapper for key-value stores used by Interactive Queries.
/// </summary>
public sealed class ReadOnlyKeyValueStoreWrapper<TKey, TValue>
    where TKey : notnull
{
    private readonly IKeyValueStore<TKey, TValue> _inner;

    internal ReadOnlyKeyValueStoreWrapper(IKeyValueStore<TKey, TValue> inner)
    {
        _inner = inner;
    }

    public TValue? Get(TKey key) => _inner.Get(key);

    public IEnumerable<KeyValue<TKey, TValue>> All() => _inner.All();

    public IEnumerable<KeyValue<TKey, TValue>> Range(TKey from, TKey to) => _inner.Range(from, to);

    public long ApproximateNumEntries => _inner.ApproximateNumEntries;
}
