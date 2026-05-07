using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Streams.Monitoring;

namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// In-memory key-value store implementation.
/// Optimized: no per-operation Stopwatch — in-memory dict ops are sub-microsecond.
/// </summary>
public sealed class InMemoryKeyValueStore<TKey, TValue> : IKeyValueStore<TKey, TValue>
    where TKey : notnull
{
    private readonly ConcurrentDictionary<TKey, TValue> _store = new();
    private readonly IComparer<TKey>? _comparer;
    private ProcessorContext? _context;
    private StateStoreMetrics? _storeMetrics;

    public string Name { get; }
    public bool Persistent => false;
    public long ApproximateNumEntries => _store.Count;

    public InMemoryKeyValueStore(string name, IComparer<TKey>? comparer = null)
    {
        Name = name;
        _comparer = comparer;
    }

    public void Init(ProcessorContext context)
    {
        _context = context;
        _storeMetrics = context.Metrics.GetOrCreateStoreMetrics(Name, () => _store.Count);
    }

    public TValue? Get(TKey key)
    {
        _store.TryGetValue(key, out var value);
        _storeMetrics?.RecordGet();
        return value;
    }

    public void Put(TKey key, TValue value)
    {
        _store[key] = value;
        _storeMetrics?.RecordPut();
    }

    public TValue? PutIfAbsent(TKey key, TValue value)
    {
        var result = _store.GetOrAdd(key, value);
        _storeMetrics?.RecordPut();
        return result;
    }

    public void PutAll(IEnumerable<KeyValue<TKey, TValue>> entries)
    {
        foreach (var entry in entries)
        {
            _store[entry.Key] = entry.Value;
            _storeMetrics?.RecordPut();
        }
    }

    public TValue? Delete(TKey key)
    {
        _store.TryRemove(key, out var value);
        _storeMetrics?.RecordDelete();
        return value;
    }

    public IEnumerable<KeyValue<TKey, TValue>> Range(TKey from, TKey to)
    {
        if (_comparer == null)
            throw new InvalidOperationException("Range queries require a comparer");

        return _store
            .Where(kv => _comparer.Compare(kv.Key, from) >= 0 && _comparer.Compare(kv.Key, to) <= 0)
            .OrderBy(kv => kv.Key, _comparer)
            .Select(kv => new KeyValue<TKey, TValue>(kv.Key, kv.Value));
    }

    public IEnumerable<KeyValue<TKey, TValue>> All()
    {
        return _store.Select(kv => new KeyValue<TKey, TValue>(kv.Key, kv.Value));
    }

    /// <summary>
    /// KIP-985: descending iteration over <c>[to, from]</c>. Requires the comparer set
    /// at construction; without it there is no defined "reverse" direction.
    /// </summary>
    public IEnumerable<KeyValue<TKey, TValue>> ReverseRange(TKey from, TKey to)
    {
        if (_comparer == null)
            throw new InvalidOperationException("ReverseRange queries require a comparer");

        // KIP-985 contract: from is the larger key, to is the smaller key. Internally
        // we still need to filter with the same low/high bounds.
        var (low, high) = _comparer.Compare(from, to) >= 0 ? (to, from) : (from, to);

        return _store
            .Where(kv => _comparer.Compare(kv.Key, low) >= 0 && _comparer.Compare(kv.Key, high) <= 0)
            .OrderByDescending(kv => kv.Key, _comparer)
            .Select(kv => new KeyValue<TKey, TValue>(kv.Key, kv.Value));
    }

    /// <summary>
    /// KIP-985: descending iteration over the full store.
    /// </summary>
    public IEnumerable<KeyValue<TKey, TValue>> ReverseAll()
    {
        var ordered = _comparer != null
            ? _store.OrderByDescending(kv => kv.Key, _comparer)
            : _store.OrderByDescending(kv => kv.Key, Comparer<TKey>.Default);

        return ordered.Select(kv => new KeyValue<TKey, TValue>(kv.Key, kv.Value));
    }

    public void Flush() { }
    public void Close() => _store.Clear();
    public void Dispose() => Close();
}
