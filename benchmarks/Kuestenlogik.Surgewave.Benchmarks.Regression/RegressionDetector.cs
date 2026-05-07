using Kuestenlogik.Surgewave.Benchmarks.Regression.Models;

namespace Kuestenlogik.Surgewave.Benchmarks.Regression;

/// <summary>
/// Compares current benchmark results against a baseline to detect regressions and improvements.
/// </summary>
public sealed class RegressionDetector
{
    private readonly RegressionConfig _config;

    public RegressionDetector(RegressionConfig config)
    {
        _config = config;
    }

    /// <summary>
    /// Compares current benchmark results against a baseline and returns classification results.
    /// </summary>
    public List<RegressionResult> Compare(
        Dictionary<string, BenchmarkEntry> baseline,
        Dictionary<string, BenchmarkEntry> current)
    {
        var results = new List<RegressionResult>();

        foreach (var (name, currentEntry) in current)
        {
            if (_config.ExcludedBenchmarks.Contains(name))
            {
                continue;
            }

            if (!baseline.TryGetValue(name, out var baselineEntry))
            {
                // New benchmark — no baseline to compare against
                results.Add(new RegressionResult
                {
                    BenchmarkName = name,
                    Metric = "New",
                    BaselineValue = 0,
                    CurrentValue = currentEntry.MeanNs,
                    DeltaPercent = 0,
                    Severity = RegressionSeverity.New,
                    Category = ExtractCategory(name)
                });
                continue;
            }

            var category = ExtractCategory(name);
            var (latencyThreshold, throughputThreshold, allocationThreshold) = GetThresholds(category);

            // Compare Mean latency (higher = regression)
            CompareMetric(results, name, "Mean (ns)",
                baselineEntry.MeanNs, currentEntry.MeanNs,
                latencyThreshold, higherIsWorse: true, category);

            // Compare Allocated bytes (higher = regression)
            if (baselineEntry.AllocatedBytes > 0 || currentEntry.AllocatedBytes > 0)
            {
                CompareMetric(results, name, "Allocated (B)",
                    baselineEntry.AllocatedBytes, currentEntry.AllocatedBytes,
                    allocationThreshold, higherIsWorse: true, category);
            }

            // Compare Operations/sec (lower = regression)
            if (baselineEntry.OperationsPerSecond > 0 && currentEntry.OperationsPerSecond > 0)
            {
                CompareMetric(results, name, "Ops/sec",
                    baselineEntry.OperationsPerSecond, currentEntry.OperationsPerSecond,
                    throughputThreshold, higherIsWorse: false, category);
            }
        }

        return results;
    }

    private static void CompareMetric(
        List<RegressionResult> results,
        string benchmarkName,
        string metricName,
        double baselineValue,
        double currentValue,
        double thresholdPercent,
        bool higherIsWorse,
        string? category)
    {
        if (baselineValue == 0)
        {
            return;
        }

        var deltaPercent = (currentValue - baselineValue) / baselineValue * 100.0;
        var severity = ClassifySeverity(deltaPercent, thresholdPercent, higherIsWorse);

        // Only report non-stable results to keep output focused
        if (severity != RegressionSeverity.Stable)
        {
            results.Add(new RegressionResult
            {
                BenchmarkName = benchmarkName,
                Metric = metricName,
                BaselineValue = baselineValue,
                CurrentValue = currentValue,
                DeltaPercent = deltaPercent,
                Severity = severity,
                Category = category
            });
        }
    }

    private static RegressionSeverity ClassifySeverity(double deltaPercent, double threshold, bool higherIsWorse)
    {
        if (higherIsWorse)
        {
            // For latency/allocations: positive delta means worse
            if (deltaPercent > threshold)
            {
                return RegressionSeverity.Regression;
            }

            if (deltaPercent < -threshold)
            {
                return RegressionSeverity.Improvement;
            }
        }
        else
        {
            // For throughput: negative delta means worse
            if (deltaPercent < -threshold)
            {
                return RegressionSeverity.Regression;
            }

            if (deltaPercent > threshold)
            {
                return RegressionSeverity.Improvement;
            }
        }

        return RegressionSeverity.Stable;
    }

    private (double latency, double throughput, double allocation) GetThresholds(string? category)
    {
        if (category is not null && _config.CategoryOverrides.TryGetValue(category, out var overrides))
        {
            return (
                overrides.LatencyThresholdPercent ?? _config.LatencyThresholdPercent,
                overrides.ThroughputThresholdPercent ?? _config.ThroughputThresholdPercent,
                overrides.AllocationThresholdPercent ?? _config.AllocationThresholdPercent
            );
        }

        return (_config.LatencyThresholdPercent, _config.ThroughputThresholdPercent, _config.AllocationThresholdPercent);
    }

    private static string? ExtractCategory(string benchmarkName)
    {
        // Extract category from fully-qualified name: "Namespace.Class.Method" -> "Class"
        var parts = benchmarkName.Split('.');
        return parts.Length >= 2 ? parts[^2] : null;
    }
}
