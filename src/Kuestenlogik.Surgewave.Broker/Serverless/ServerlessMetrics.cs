using System.Diagnostics.Metrics;

namespace Kuestenlogik.Surgewave.Broker.Serverless;

/// <summary>
/// OpenTelemetry-compatible metrics for serverless scaling operations.
/// Uses <see cref="System.Diagnostics.Metrics.Meter"/> following the same
/// patterns as <see cref="BrokerMetrics"/>.
/// </summary>
public sealed class ServerlessMetrics : IDisposable
{
    public const string MeterName = "Kuestenlogik.Surgewave.Serverless";

    private readonly Meter _meter;

    // === Counters ===
    private readonly Counter<long> _scaleUpTotal;
    private readonly Counter<long> _scaleDownTotal;
    private readonly Counter<long> _drainTotal;
    private readonly Counter<long> _coldStartsTotal;

    // === Histograms ===
    private readonly Histogram<double> _drainDurationMs;
    private readonly Histogram<double> _coldStartDurationMs;

    // === Observable Gauges ===
    private readonly ObservableGauge<int> _brokerState;
    private readonly ObservableGauge<long> _unflushedBytes;
    private readonly ObservableGauge<int> _activeBrokers;

    // State accessors for observable gauges
    private Func<int>? _getBrokerState;
    private Func<long>? _getUnflushedBytes;
    private Func<int>? _getActiveBrokers;

    public ServerlessMetrics()
    {
        _meter = new Meter(MeterName, "1.0.0");

        // === Counters ===
        _scaleUpTotal = _meter.CreateCounter<long>(
            "surgewave_serverless_scale_up_total",
            description: "Total number of scale-up events");

        _scaleDownTotal = _meter.CreateCounter<long>(
            "surgewave_serverless_scale_down_total",
            description: "Total number of scale-down events");

        _drainTotal = _meter.CreateCounter<long>(
            "surgewave_serverless_drain_total",
            description: "Total number of drain operations initiated");

        _coldStartsTotal = _meter.CreateCounter<long>(
            "surgewave_serverless_cold_starts_total",
            description: "Total number of cold start events");

        // === Histograms ===
        _drainDurationMs = _meter.CreateHistogram<double>(
            "surgewave_serverless_drain_duration_ms",
            unit: "ms",
            description: "Duration of drain operations in milliseconds");

        _coldStartDurationMs = _meter.CreateHistogram<double>(
            "surgewave_serverless_cold_start_duration_ms",
            unit: "ms",
            description: "Duration of cold start operations in milliseconds");

        // === Observable Gauges ===
        _brokerState = _meter.CreateObservableGauge(
            "surgewave_serverless_broker_state",
            () => _getBrokerState?.Invoke() ?? 0,
            description: "Current broker lifecycle state encoded as integer");

        _unflushedBytes = _meter.CreateObservableGauge(
            "surgewave_serverless_unflushed_bytes",
            () => _getUnflushedBytes?.Invoke() ?? 0,
            unit: "By",
            description: "Total bytes in write buffers not yet flushed to object storage");

        _activeBrokers = _meter.CreateObservableGauge(
            "surgewave_serverless_active_brokers",
            () => _getActiveBrokers?.Invoke() ?? 0,
            description: "Number of currently active broker instances");
    }

    /// <summary>
    /// Register callbacks for observable gauges (pull-based metrics).
    /// </summary>
    public void RegisterStateAccessors(
        Func<int> getBrokerState,
        Func<long> getUnflushedBytes,
        Func<int> getActiveBrokers)
    {
        _getBrokerState = getBrokerState;
        _getUnflushedBytes = getUnflushedBytes;
        _getActiveBrokers = getActiveBrokers;
    }

    /// <summary>Record a scale-up event.</summary>
    public void RecordScaleUp() => _scaleUpTotal.Add(1);

    /// <summary>Record a scale-down event.</summary>
    public void RecordScaleDown() => _scaleDownTotal.Add(1);

    /// <summary>Record a drain operation initiated.</summary>
    public void RecordDrain() => _drainTotal.Add(1);

    /// <summary>Record a cold start event.</summary>
    public void RecordColdStart() => _coldStartsTotal.Add(1);

    /// <summary>Record the duration of a drain operation in milliseconds.</summary>
    public void RecordDrainDuration(double durationMs) =>
        _drainDurationMs.Record(durationMs);

    /// <summary>Record the duration of a cold start in milliseconds.</summary>
    public void RecordColdStartDuration(double durationMs) =>
        _coldStartDurationMs.Record(durationMs);

    public void Dispose()
    {
        _meter.Dispose();
    }
}
