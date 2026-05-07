using System.Diagnostics.Metrics;

namespace Kuestenlogik.Surgewave.Streams.Monitoring;

/// <summary>
/// Per-state-store metrics with OTEL instrumentation.
/// </summary>
public sealed class StateStoreMetrics
{
    private readonly Counter<long> _puts;
    private readonly Counter<long> _gets;
    private readonly Counter<long> _deletes;
    private readonly Counter<long> _flushes;
    private readonly Histogram<double> _putLatency;
    private readonly Histogram<double> _getLatency;
    private readonly Histogram<double> _deleteLatency;
    private readonly Histogram<double> _flushLatency;
    private readonly KeyValuePair<string, object?> _storeTag;

    private long _totalPuts;
    private long _totalGets;
    private long _totalDeletes;
    private long _totalFlushes;
    private long _totalRestored;

    public string StoreName { get; }
    public long TotalPuts => _totalPuts;
    public long TotalGets => _totalGets;
    public long TotalDeletes => _totalDeletes;
    public long TotalFlushes => _totalFlushes;
    public long TotalRestored => _totalRestored;

    public StateStoreMetrics(Meter meter, string storeName, Func<long>? entryCountProvider = null)
    {
        StoreName = storeName;
        _storeTag = new KeyValuePair<string, object?>("store.name", storeName);

        _puts = meter.CreateCounter<long>(
            "surgewave_streams_store_puts_total",
            description: "Total put operations on state store");

        _gets = meter.CreateCounter<long>(
            "surgewave_streams_store_gets_total",
            description: "Total get operations on state store");

        _deletes = meter.CreateCounter<long>(
            "surgewave_streams_store_deletes_total",
            description: "Total delete operations on state store");

        _flushes = meter.CreateCounter<long>(
            "surgewave_streams_store_flushes_total",
            description: "Total flush operations on state store");

        _putLatency = meter.CreateHistogram<double>(
            "surgewave_streams_store_put_latency_ms",
            unit: "ms",
            description: "Put latency per state store");

        _getLatency = meter.CreateHistogram<double>(
            "surgewave_streams_store_get_latency_ms",
            unit: "ms",
            description: "Get latency per state store");

        _deleteLatency = meter.CreateHistogram<double>(
            "surgewave_streams_store_delete_latency_ms",
            unit: "ms",
            description: "Delete latency per state store");

        _flushLatency = meter.CreateHistogram<double>(
            "surgewave_streams_store_flush_latency_ms",
            unit: "ms",
            description: "Flush latency per state store");

        meter.CreateObservableGauge(
            "surgewave_streams_store_restored_records_total",
            () => Interlocked.Read(ref _totalRestored),
            description: "Total records restored from changelog per store");

        if (entryCountProvider != null)
        {
            meter.CreateObservableGauge(
                "surgewave_streams_store_entries",
                entryCountProvider,
                description: "Number of entries in state store");
        }
    }

    public void RecordPut(double latencyMs = 0)
    {
        Interlocked.Increment(ref _totalPuts);
        _puts.Add(1, _storeTag);
        if (latencyMs > 0)
            _putLatency.Record(latencyMs, _storeTag);
    }

    public void RecordPut(int count)
    {
        Interlocked.Add(ref _totalPuts, count);
        _puts.Add(count, _storeTag);
    }

    public void RecordGet(double latencyMs = 0)
    {
        Interlocked.Increment(ref _totalGets);
        _gets.Add(1, _storeTag);
        if (latencyMs > 0)
            _getLatency.Record(latencyMs, _storeTag);
    }

    public void RecordDelete(double latencyMs = 0)
    {
        Interlocked.Increment(ref _totalDeletes);
        _deletes.Add(1, _storeTag);
        if (latencyMs > 0)
            _deleteLatency.Record(latencyMs, _storeTag);
    }

    /// <summary>Records a flush operation with optional latency measurement.</summary>
    /// <param name="latencyMs">The flush duration in milliseconds (0 to skip latency recording).</param>
    public void RecordFlush(double latencyMs = 0)
    {
        Interlocked.Increment(ref _totalFlushes);
        _flushes.Add(1, _storeTag);
        if (latencyMs > 0)
            _flushLatency.Record(latencyMs, _storeTag);
    }

    /// <summary>Records that records were restored from the changelog into this store.</summary>
    /// <param name="records">The number of records restored.</param>
    public void RecordRestore(long records)
    {
        Interlocked.Add(ref _totalRestored, records);
    }
}
