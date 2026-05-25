namespace Kuestenlogik.Surgewave.Benchmarks.RealWorld;

/// <summary>
/// Collects latency samples and computes percentiles for benchmark reporting.
/// Not thread-safe; designed for single-writer usage within a scenario.
/// </summary>
public sealed class LatencyHistogram
{
    private readonly List<double> _samples = [];

    /// <summary>Records a latency sample in microseconds.</summary>
    public void RecordMicroseconds(double us)
    {
        _samples.Add(us);
    }

    /// <summary>Records a latency sample in milliseconds.</summary>
    public void RecordMilliseconds(double ms)
    {
        _samples.Add(ms * 1000.0);
    }

    /// <summary>Number of samples recorded.</summary>
    public int Count => _samples.Count;

    /// <summary>
    /// Computes a specific percentile (0-100) from recorded samples.
    /// Returns the value in microseconds.
    /// </summary>
    public double PercentileMicroseconds(double percentile)
    {
        if (_samples.Count == 0)
            return 0;

        _samples.Sort();
        var index = (int)Math.Ceiling(percentile / 100.0 * _samples.Count) - 1;
        return _samples[Math.Max(0, Math.Min(index, _samples.Count - 1))];
    }

    /// <summary>
    /// Computes a specific percentile (0-100) and returns the value in milliseconds.
    /// </summary>
    public double PercentileMilliseconds(double percentile)
    {
        return PercentileMicroseconds(percentile) / 1000.0;
    }

    /// <summary>Mean latency in microseconds.</summary>
    public double MeanMicroseconds()
    {
        return _samples.Count == 0 ? 0 : _samples.Sum() / _samples.Count;
    }

    /// <summary>Mean latency in milliseconds.</summary>
    public double MeanMilliseconds()
    {
        return MeanMicroseconds() / 1000.0;
    }

    /// <summary>
    /// Populates a dictionary with standard percentile metrics (P50, P90, P99, P99.9, P99.99) in milliseconds.
    /// </summary>
    /// <param name="metrics">Target dictionary.</param>
    /// <param name="prefix">Metric key prefix (e.g. "produce_").</param>
    public void PopulateMetrics(Dictionary<string, double> metrics, string prefix)
    {
        metrics[$"{prefix}p50_ms"] = PercentileMilliseconds(50);
        metrics[$"{prefix}p90_ms"] = PercentileMilliseconds(90);
        metrics[$"{prefix}p99_ms"] = PercentileMilliseconds(99);
        metrics[$"{prefix}p99_9_ms"] = PercentileMilliseconds(99.9);
        metrics[$"{prefix}p99_99_ms"] = PercentileMilliseconds(99.99);
        metrics[$"{prefix}mean_ms"] = MeanMilliseconds();
    }
}
