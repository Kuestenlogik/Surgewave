namespace Kuestenlogik.Surgewave.Benchmarks.Regression.Models;

/// <summary>
/// Represents the result of comparing a single benchmark metric against its baseline.
/// </summary>
public sealed record RegressionResult
{
    /// <summary>Fully-qualified benchmark name.</summary>
    public required string BenchmarkName { get; init; }

    /// <summary>The metric being compared (e.g., "Mean (ns)", "Allocated (B)").</summary>
    public required string Metric { get; init; }

    /// <summary>The baseline value for this metric.</summary>
    public double BaselineValue { get; init; }

    /// <summary>The current measured value for this metric.</summary>
    public double CurrentValue { get; init; }

    /// <summary>Percentage change from baseline: (current - baseline) / baseline * 100.</summary>
    public double DeltaPercent { get; init; }

    /// <summary>Classification of this comparison result.</summary>
    public required RegressionSeverity Severity { get; init; }

    /// <summary>Optional benchmark category for threshold overrides.</summary>
    public string? Category { get; init; }
}
