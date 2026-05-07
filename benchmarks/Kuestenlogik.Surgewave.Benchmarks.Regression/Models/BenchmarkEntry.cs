namespace Kuestenlogik.Surgewave.Benchmarks.Regression.Models;

/// <summary>
/// Represents a single benchmark measurement with its key metrics.
/// </summary>
public sealed class BenchmarkEntry
{
    /// <summary>Mean execution time in nanoseconds.</summary>
    public double MeanNs { get; set; }

    /// <summary>Median execution time in nanoseconds.</summary>
    public double MedianNs { get; set; }

    /// <summary>Standard deviation of execution time in nanoseconds.</summary>
    public double StdDevNs { get; set; }

    /// <summary>Bytes allocated per operation.</summary>
    public long AllocatedBytes { get; set; }

    /// <summary>Operations per second (derived from MeanNs).</summary>
    public double OperationsPerSecond { get; set; }
}
