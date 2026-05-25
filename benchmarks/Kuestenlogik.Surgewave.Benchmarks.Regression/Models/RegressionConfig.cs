namespace Kuestenlogik.Surgewave.Benchmarks.Regression.Models;

/// <summary>
/// Configuration for regression detection thresholds and exclusions.
/// </summary>
public sealed class RegressionConfig
{
    /// <summary>
    /// Percentage threshold for latency regression (mean execution time).
    /// A benchmark is flagged as regressed if its mean time increases by more than this percentage.
    /// </summary>
    public double LatencyThresholdPercent { get; set; } = 15.0;

    /// <summary>
    /// Percentage threshold for throughput regression (operations per second).
    /// A benchmark is flagged as regressed if its throughput drops by more than this percentage.
    /// </summary>
    public double ThroughputThresholdPercent { get; set; } = 10.0;

    /// <summary>
    /// Percentage threshold for allocation regression (bytes allocated per operation).
    /// A benchmark is flagged as regressed if its allocations increase by more than this percentage.
    /// </summary>
    public double AllocationThresholdPercent { get; set; } = 20.0;

    /// <summary>
    /// Set of benchmark names to exclude from regression checks.
    /// </summary>
    public HashSet<string> ExcludedBenchmarks { get; set; } = [];

    /// <summary>
    /// Per-category threshold overrides. The category is derived from the benchmark's
    /// BenchmarkCategory attribute or the class name.
    /// </summary>
    public Dictionary<string, CategoryThresholds> CategoryOverrides { get; set; } = [];
}

/// <summary>
/// Per-category threshold overrides. Null values fall back to the global defaults.
/// </summary>
public sealed class CategoryThresholds
{
    /// <summary>Override for latency threshold percent, or null to use global.</summary>
    public double? LatencyThresholdPercent { get; set; }

    /// <summary>Override for throughput threshold percent, or null to use global.</summary>
    public double? ThroughputThresholdPercent { get; set; }

    /// <summary>Override for allocation threshold percent, or null to use global.</summary>
    public double? AllocationThresholdPercent { get; set; }
}
