using Kuestenlogik.Surgewave.Streams.Windows;

namespace Kuestenlogik.Surgewave.Streams.InteractiveQueries;

/// <summary>
/// Read-only wrapper for window stores used by Interactive Queries.
/// </summary>
public sealed class ReadOnlyWindowStoreWrapper<TKey, TValue>
    where TKey : notnull
{
    private readonly IWindowStore<TKey, TValue> _inner;

    internal ReadOnlyWindowStoreWrapper(IWindowStore<TKey, TValue> inner)
    {
        _inner = inner;
    }

    public TValue? Fetch(TKey key, long windowStartMs) => _inner.Fetch(key, windowStartMs);

    public IEnumerable<KeyValue<Windowed<TKey>, TValue>> Fetch(TKey key, long timeFrom, long timeTo)
        => _inner.Fetch(key, timeFrom, timeTo);

    public IEnumerable<KeyValue<Windowed<TKey>, TValue>> FetchAll(long timeFrom, long timeTo)
        => _inner.FetchAll(timeFrom, timeTo);
}
