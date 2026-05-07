using System.Diagnostics.Metrics;

namespace Kuestenlogik.Surgewave.Streams.Monitoring;

/// <summary>
/// Per-window-store metrics with OTEL instrumentation.
/// Tracks puts, fetches, and expiry events for windowed state stores.
/// </summary>
public sealed class WindowStoreMetrics
{
    private readonly Counter<long> _puts;
    private readonly Counter<long> _fetches;
    private readonly Counter<long> _expired;
    private readonly Histogram<double> _putLatency;
    private readonly Histogram<double> _fetchLatency;
    private readonly KeyValuePair<string, object?> _storeTag;

    private long _totalPuts;
    private long _totalFetches;
    private long _totalExpired;

    public string StoreName { get; }
    public long TotalPuts => _totalPuts;
    public long TotalFetches => _totalFetches;
    public long TotalExpired => _totalExpired;

    public WindowStoreMetrics(Meter meter, string storeName)
    {
        StoreName = storeName;
        _storeTag = new KeyValuePair<string, object?>("store.name", storeName);

        _puts = meter.CreateCounter<long>(
            "surgewave_streams_window_store_puts_total",
            description: "Total put operations on window store");

        _fetches = meter.CreateCounter<long>(
            "surgewave_streams_window_store_fetches_total",
            description: "Total fetch operations on window store");

        _expired = meter.CreateCounter<long>(
            "surgewave_streams_window_store_expired_total",
            description: "Total windows expired from window store");

        _putLatency = meter.CreateHistogram<double>(
            "surgewave_streams_window_store_put_latency_ms",
            unit: "ms",
            description: "Put latency per window store");

        _fetchLatency = meter.CreateHistogram<double>(
            "surgewave_streams_window_store_fetch_latency_ms",
            unit: "ms",
            description: "Fetch latency per window store");
    }

    /// <summary>Records a put operation on the window store.</summary>
    /// <param name="latencyMs">The put duration in milliseconds (0 to skip latency recording).</param>
    public void RecordPut(double latencyMs = 0)
    {
        Interlocked.Increment(ref _totalPuts);
        _puts.Add(1, _storeTag);
        if (latencyMs > 0)
            _putLatency.Record(latencyMs, _storeTag);
    }

    /// <summary>Records a fetch operation on the window store.</summary>
    /// <param name="latencyMs">The fetch duration in milliseconds (0 to skip latency recording).</param>
    public void RecordFetch(double latencyMs = 0)
    {
        Interlocked.Increment(ref _totalFetches);
        _fetches.Add(1, _storeTag);
        if (latencyMs > 0)
            _fetchLatency.Record(latencyMs, _storeTag);
    }

    /// <summary>Records that one or more windows were expired from the store.</summary>
    /// <param name="count">Number of expired windows.</param>
    public void RecordExpired(int count = 1)
    {
        Interlocked.Add(ref _totalExpired, count);
        _expired.Add(count, _storeTag);
    }
}
