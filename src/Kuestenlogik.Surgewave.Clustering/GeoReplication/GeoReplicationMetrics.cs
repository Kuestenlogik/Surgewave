using System.Diagnostics.Metrics;

namespace Kuestenlogik.Surgewave.Clustering.GeoReplication;

/// <summary>
/// Prometheus-compatible metrics for geo-replication.
/// Uses System.Diagnostics.Metrics for OpenTelemetry integration.
/// </summary>
public sealed class GeoReplicationMetrics : IDisposable
{
    private readonly Meter _meter;
    private readonly Histogram<long> _lagMessages;
    private readonly Histogram<double> _lagMs;
    private readonly Counter<long> _bytesTotal;
    private readonly Counter<long> _fetchRate;
    private readonly Counter<long> _errorsTotal;

    public GeoReplicationMetrics()
    {
        _meter = new Meter("Kuestenlogik.Surgewave.GeoReplication", "1.0.0");

        _lagMessages = _meter.CreateHistogram<long>(
            "surgewave_geo_replication_lag_messages",
            "messages",
            "Replication lag in messages per partition");

        _lagMs = _meter.CreateHistogram<double>(
            "surgewave_geo_replication_lag_ms",
            "ms",
            "Replication lag in milliseconds per partition");

        _bytesTotal = _meter.CreateCounter<long>(
            "surgewave_geo_replication_bytes_total",
            "bytes",
            "Total bytes replicated per topic");

        _fetchRate = _meter.CreateCounter<long>(
            "surgewave_geo_replication_fetch_rate",
            "fetches",
            "Number of fetch operations per link");

        _errorsTotal = _meter.CreateCounter<long>(
            "surgewave_geo_replication_errors_total",
            "errors",
            "Total replication errors per link");
    }

    public void RecordLagMessages(string linkId, string topic, int partition, long lag)
    {
        _lagMessages.Record(lag,
            new KeyValuePair<string, object?>("link", linkId),
            new KeyValuePair<string, object?>("topic", topic),
            new KeyValuePair<string, object?>("partition", partition));
    }

    public void RecordLagMs(string linkId, string topic, int partition, double lagMs)
    {
        _lagMs.Record(lagMs,
            new KeyValuePair<string, object?>("link", linkId),
            new KeyValuePair<string, object?>("topic", topic),
            new KeyValuePair<string, object?>("partition", partition));
    }

    public void RecordBytesReplicated(string linkId, string topic, long bytes)
    {
        _bytesTotal.Add(bytes,
            new KeyValuePair<string, object?>("link", linkId),
            new KeyValuePair<string, object?>("topic", topic));
    }

    public void RecordFetch(string linkId)
    {
        _fetchRate.Add(1,
            new KeyValuePair<string, object?>("link", linkId));
    }

    public void RecordError(string linkId)
    {
        _errorsTotal.Add(1,
            new KeyValuePair<string, object?>("link", linkId));
    }

    public void Dispose() => _meter.Dispose();
}
