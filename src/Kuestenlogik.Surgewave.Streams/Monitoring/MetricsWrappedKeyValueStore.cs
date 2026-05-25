using System.Diagnostics;

namespace Kuestenlogik.Surgewave.Streams.Monitoring;

/// <summary>
/// Decorator that wraps an <see cref="IKeyValueStore{TKey,TValue}"/> and instruments all
/// operations with <see cref="StateStoreMetrics"/> latency and counter measurements.
/// </summary>
/// <typeparam name="TKey">The key type.</typeparam>
/// <typeparam name="TValue">The value type.</typeparam>
public sealed class MetricsWrappedKeyValueStore<TKey, TValue> : IKeyValueStore<TKey, TValue>
    where TKey : notnull
{
    private readonly IKeyValueStore<TKey, TValue> _inner;
    private readonly StateStoreMetrics _metrics;

    public MetricsWrappedKeyValueStore(IKeyValueStore<TKey, TValue> inner, StateStoreMetrics metrics)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
    }

    public string Name => _inner.Name;
    public bool Persistent => _inner.Persistent;
    public long ApproximateNumEntries => _inner.ApproximateNumEntries;

    public void Init(ProcessorContext context) => _inner.Init(context);

    public TValue? Get(TKey key)
    {
        var sw = Stopwatch.GetTimestamp();
        var result = _inner.Get(key);
        _metrics.RecordGet(ElapsedMs(sw));
        return result;
    }

    public void Put(TKey key, TValue value)
    {
        var sw = Stopwatch.GetTimestamp();
        _inner.Put(key, value);
        _metrics.RecordPut(ElapsedMs(sw));
    }

    public TValue? PutIfAbsent(TKey key, TValue value)
    {
        var sw = Stopwatch.GetTimestamp();
        var result = _inner.PutIfAbsent(key, value);
        _metrics.RecordPut(ElapsedMs(sw));
        return result;
    }

    public void PutAll(IEnumerable<KeyValue<TKey, TValue>> entries)
    {
        var list = entries as IReadOnlyList<KeyValue<TKey, TValue>> ?? entries.ToList();
        var sw = Stopwatch.GetTimestamp();
        _inner.PutAll(list);
        var latencyMs = ElapsedMs(sw);
        // Record batch count first, then latency on a single representative put
        _metrics.RecordPut(list.Count);
        if (list.Count > 0 && latencyMs > 0)
            _metrics.RecordPut(latencyMs / list.Count);
    }

    public TValue? Delete(TKey key)
    {
        var sw = Stopwatch.GetTimestamp();
        var result = _inner.Delete(key);
        _metrics.RecordDelete(ElapsedMs(sw));
        return result;
    }

    public IEnumerable<KeyValue<TKey, TValue>> Range(TKey from, TKey to)
        => _inner.Range(from, to);

    public IEnumerable<KeyValue<TKey, TValue>> All()
        => _inner.All();

    public void Flush()
    {
        var sw = Stopwatch.GetTimestamp();
        _inner.Flush();
        _metrics.RecordFlush(ElapsedMs(sw));
    }

    public void Close() => _inner.Close();

    public void Dispose() => _inner.Dispose();

    private static double ElapsedMs(long start)
    {
        var ticks = Stopwatch.GetTimestamp() - start;
        return ticks * 1000.0 / Stopwatch.Frequency;
    }
}
