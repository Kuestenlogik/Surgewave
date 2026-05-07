using System.Diagnostics.Metrics;

namespace Kuestenlogik.Surgewave.Streams.Monitoring;

/// <summary>
/// Per-session-store metrics with OTEL instrumentation.
/// Tracks puts, fetches, removals, and merges for session-windowed state stores.
/// </summary>
public sealed class SessionStoreMetrics
{
    private readonly Counter<long> _puts;
    private readonly Counter<long> _fetches;
    private readonly Counter<long> _removes;
    private readonly Counter<long> _merges;
    private readonly Histogram<double> _putLatency;
    private readonly Histogram<double> _fetchLatency;
    private readonly KeyValuePair<string, object?> _storeTag;

    private long _totalPuts;
    private long _totalFetches;
    private long _totalRemoves;
    private long _totalMerges;

    public string StoreName { get; }
    public long TotalPuts => _totalPuts;
    public long TotalFetches => _totalFetches;
    public long TotalRemoves => _totalRemoves;
    public long TotalMerges => _totalMerges;

    public SessionStoreMetrics(Meter meter, string storeName)
    {
        StoreName = storeName;
        _storeTag = new KeyValuePair<string, object?>("store.name", storeName);

        _puts = meter.CreateCounter<long>(
            "surgewave_streams_session_store_puts_total",
            description: "Total put operations on session store");

        _fetches = meter.CreateCounter<long>(
            "surgewave_streams_session_store_fetches_total",
            description: "Total fetch operations on session store");

        _removes = meter.CreateCounter<long>(
            "surgewave_streams_session_store_removes_total",
            description: "Total remove operations on session store");

        _merges = meter.CreateCounter<long>(
            "surgewave_streams_session_store_merges_total",
            description: "Total session merge operations on session store");

        _putLatency = meter.CreateHistogram<double>(
            "surgewave_streams_session_store_put_latency_ms",
            unit: "ms",
            description: "Put latency per session store");

        _fetchLatency = meter.CreateHistogram<double>(
            "surgewave_streams_session_store_fetch_latency_ms",
            unit: "ms",
            description: "Fetch latency per session store");
    }

    /// <summary>Records a put (or session merge-put) operation on the session store.</summary>
    /// <param name="latencyMs">The put duration in milliseconds (0 to skip latency recording).</param>
    public void RecordPut(double latencyMs = 0)
    {
        Interlocked.Increment(ref _totalPuts);
        _puts.Add(1, _storeTag);
        if (latencyMs > 0)
            _putLatency.Record(latencyMs, _storeTag);
    }

    /// <summary>Records a fetch/find operation on the session store.</summary>
    /// <param name="latencyMs">The fetch duration in milliseconds (0 to skip latency recording).</param>
    public void RecordFetch(double latencyMs = 0)
    {
        Interlocked.Increment(ref _totalFetches);
        _fetches.Add(1, _storeTag);
        if (latencyMs > 0)
            _fetchLatency.Record(latencyMs, _storeTag);
    }

    /// <summary>Records a remove operation on the session store.</summary>
    public void RecordRemove()
    {
        Interlocked.Increment(ref _totalRemoves);
        _removes.Add(1, _storeTag);
    }

    /// <summary>Records a session merge event (two sessions merged into one).</summary>
    public void RecordMerge()
    {
        Interlocked.Increment(ref _totalMerges);
        _merges.Add(1, _storeTag);
    }
}
