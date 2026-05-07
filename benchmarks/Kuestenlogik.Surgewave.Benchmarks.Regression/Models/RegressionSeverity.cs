namespace Kuestenlogik.Surgewave.Benchmarks.Regression.Models;

/// <summary>
/// Indicates the severity classification of a benchmark comparison result.
/// </summary>
public enum RegressionSeverity
{
    /// <summary>Benchmark is within acceptable thresholds.</summary>
    Stable,

    /// <summary>Benchmark improved beyond the threshold (positive change).</summary>
    Improvement,

    /// <summary>Benchmark regressed beyond the threshold (negative change).</summary>
    Regression,

    /// <summary>Benchmark is new and has no baseline for comparison.</summary>
    New
}
