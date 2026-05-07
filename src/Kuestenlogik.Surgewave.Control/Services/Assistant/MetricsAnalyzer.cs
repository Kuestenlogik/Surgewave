using Kuestenlogik.Surgewave.Control.Models.Assistant;

namespace Kuestenlogik.Surgewave.Control.Services.Assistant;

/// <summary>
/// Rule-based anomaly detection over a rolling window of metrics snapshots.
/// Thread-safe for use within a scoped Blazor circuit.
/// </summary>
public sealed class MetricsAnalyzer : IMetricsAnalyzer
{
    private const int MaxBufferSize = 60;

    private readonly Lock _lock = new();
    private readonly List<MetricsSnapshot> _buffer = new(MaxBufferSize);
    private readonly List<long> _lagHistory = new(MaxBufferSize);

    /// <inheritdoc />
    public Task<List<AnomalyDetection>> AnalyzeAsync(MetricsSnapshot snapshot, double sensitivity = 0.5)
    {
        lock (_lock)
        {
            if (_buffer.Count >= MaxBufferSize)
            {
                _buffer.RemoveAt(0);
            }
            _buffer.Add(snapshot);

            if (_lagHistory.Count >= MaxBufferSize)
            {
                _lagHistory.RemoveAt(0);
            }
            _lagHistory.Add(snapshot.MaxConsumerLag);
        }

        var anomalies = new List<AnomalyDetection>();

        // Sensitivity scales thresholds: higher sensitivity = stricter (lower thresholds)
        // At 0.0 sensitivity, multiplier is 2.0 (very lenient)
        // At 0.5 sensitivity, multiplier is 1.0 (baseline)
        // At 1.0 sensitivity, multiplier is 0.5 (very strict)
        var thresholdMultiplier = 2.0 - (sensitivity * 1.5);

        CheckThroughputDrop(snapshot, anomalies, thresholdMultiplier);
        CheckLatencySpike(snapshot, anomalies, thresholdMultiplier);
        CheckErrorRate(snapshot, anomalies, thresholdMultiplier);
        CheckConsumerLagGrowing(snapshot, anomalies);
        CheckConnectionSaturation(snapshot, anomalies, thresholdMultiplier);

        return Task.FromResult(anomalies);
    }

    private void CheckThroughputDrop(MetricsSnapshot snapshot, List<AnomalyDetection> anomalies, double multiplier)
    {
        List<MetricsSnapshot> recentSnapshots;
        lock (_lock)
        {
            if (_buffer.Count < 5) return;
            recentSnapshots = _buffer.Skip(Math.Max(0, _buffer.Count - 6)).Take(5).ToList();
        }

        var avgProduced = recentSnapshots.Average(s => s.MessagesProducedTotal);
        if (avgProduced <= 0) return;

        // ThroughputDrop: current < 50% of 5-snapshot average (scaled by sensitivity)
        var threshold = 0.5 * multiplier;
        var ratio = snapshot.MessagesProducedTotal / avgProduced;

        if (ratio < threshold)
        {
            var deviation = (1.0 - ratio) * 100.0;
            var severity = deviation > 80 ? "Critical" : deviation > 50 ? "Warning" : "Info";
            anomalies.Add(new AnomalyDetection
            {
                Type = "ThroughputDrop",
                Severity = severity,
                Resource = "cluster",
                Description = $"Message throughput dropped to {ratio:P0} of the recent average ({avgProduced:N0} msgs). " +
                              $"Current: {snapshot.MessagesProducedTotal:N0} msgs.",
                CurrentValue = snapshot.MessagesProducedTotal,
                BaselineValue = avgProduced,
                DeviationPercent = deviation
            });
        }
    }

    private static void CheckLatencySpike(MetricsSnapshot snapshot, List<AnomalyDetection> anomalies, double multiplier)
    {
        // LatencySpike: P99 > 3x P50 (scaled by sensitivity)
        var spikeThreshold = 3.0 * multiplier;

        if (snapshot.ProduceLatencyP50 > 0 && snapshot.ProduceLatencyP99 > snapshot.ProduceLatencyP50 * spikeThreshold)
        {
            var ratio = snapshot.ProduceLatencyP99 / snapshot.ProduceLatencyP50;
            anomalies.Add(new AnomalyDetection
            {
                Type = "LatencySpike",
                Severity = ratio > 10 ? "Critical" : "Warning",
                Resource = "produce",
                Description = $"Produce P99 latency ({snapshot.ProduceLatencyP99:F1}ms) is {ratio:F1}x the P50 ({snapshot.ProduceLatencyP50:F1}ms). " +
                              "This indicates tail-latency outliers.",
                CurrentValue = snapshot.ProduceLatencyP99,
                BaselineValue = snapshot.ProduceLatencyP50,
                DeviationPercent = (ratio - 1.0) * 100.0
            });
        }

        if (snapshot.FetchLatencyP50 > 0 && snapshot.FetchLatencyP99 > snapshot.FetchLatencyP50 * spikeThreshold)
        {
            var ratio = snapshot.FetchLatencyP99 / snapshot.FetchLatencyP50;
            anomalies.Add(new AnomalyDetection
            {
                Type = "LatencySpike",
                Severity = ratio > 10 ? "Critical" : "Warning",
                Resource = "fetch",
                Description = $"Fetch P99 latency ({snapshot.FetchLatencyP99:F1}ms) is {ratio:F1}x the P50 ({snapshot.FetchLatencyP50:F1}ms). " +
                              "This indicates tail-latency outliers.",
                CurrentValue = snapshot.FetchLatencyP99,
                BaselineValue = snapshot.FetchLatencyP50,
                DeviationPercent = (ratio - 1.0) * 100.0
            });
        }
    }

    private static void CheckErrorRate(MetricsSnapshot snapshot, List<AnomalyDetection> anomalies, double multiplier)
    {
        // ErrorRateHigh: error rate > 1% of total requests (scaled by sensitivity)
        if (snapshot.RequestsTotal <= 0) return;

        var errorRate = (double)snapshot.ErrorsTotal / snapshot.RequestsTotal;
        var threshold = 0.01 * multiplier;

        if (errorRate > threshold)
        {
            var pct = errorRate * 100.0;
            anomalies.Add(new AnomalyDetection
            {
                Type = "ErrorRateHigh",
                Severity = pct > 5 ? "Critical" : pct > 2 ? "Warning" : "Info",
                Resource = "cluster",
                Description = $"Error rate is {pct:F2}% ({snapshot.ErrorsTotal:N0} errors out of {snapshot.RequestsTotal:N0} requests). " +
                              $"Threshold: {threshold * 100.0:F2}%.",
                CurrentValue = pct,
                BaselineValue = threshold * 100.0,
                DeviationPercent = (errorRate / threshold - 1.0) * 100.0
            });
        }
    }

    private void CheckConsumerLagGrowing(MetricsSnapshot snapshot, List<AnomalyDetection> anomalies)
    {
        // ConsumerLagGrowing: lag increasing for 3+ consecutive snapshots
        List<long> recentLags;
        lock (_lock)
        {
            if (_lagHistory.Count < 3) return;
            recentLags = _lagHistory.Skip(Math.Max(0, _lagHistory.Count - 4)).ToList();
        }

        if (recentLags.Count < 3) return;

        var isGrowing = true;
        for (var i = 1; i < recentLags.Count; i++)
        {
            if (recentLags[i] <= recentLags[i - 1])
            {
                isGrowing = false;
                break;
            }
        }

        if (isGrowing && snapshot.MaxConsumerLag > 0)
        {
            var firstLag = recentLags[0];
            var growth = firstLag > 0 ? ((double)snapshot.MaxConsumerLag / firstLag - 1.0) * 100.0 : 100.0;

            anomalies.Add(new AnomalyDetection
            {
                Type = "ConsumerLagGrowing",
                Severity = snapshot.MaxConsumerLag > 100_000 ? "Critical" : snapshot.MaxConsumerLag > 10_000 ? "Warning" : "Info",
                Resource = "consumer-groups",
                Description = $"Consumer lag has been growing for {recentLags.Count} consecutive snapshots. " +
                              $"Current max lag: {snapshot.MaxConsumerLag:N0}, up {growth:F0}% from {firstLag:N0}.",
                CurrentValue = snapshot.MaxConsumerLag,
                BaselineValue = firstLag,
                DeviationPercent = growth
            });
        }
    }

    private static void CheckConnectionSaturation(MetricsSnapshot snapshot, List<AnomalyDetection> anomalies, double multiplier)
    {
        // ConnectionSaturation: connections > 80% of max (using 10_000 as a reasonable default max)
        const int assumedMaxConnections = 10_000;
        var threshold = 0.80 * multiplier;
        var ratio = (double)snapshot.ActiveConnections / assumedMaxConnections;

        if (ratio > threshold)
        {
            anomalies.Add(new AnomalyDetection
            {
                Type = "ConnectionSaturation",
                Severity = ratio > 0.95 ? "Critical" : "Warning",
                Resource = "cluster",
                Description = $"Active connections ({snapshot.ActiveConnections:N0}) are at {ratio:P0} of estimated capacity ({assumedMaxConnections:N0}). " +
                              "Consider scaling out or closing idle connections.",
                CurrentValue = snapshot.ActiveConnections,
                BaselineValue = assumedMaxConnections,
                DeviationPercent = (ratio - threshold) / threshold * 100.0
            });
        }
    }
}
