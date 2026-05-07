using System.Runtime.InteropServices;

namespace Kuestenlogik.Surgewave.Benchmarks.RealWorld;

/// <summary>
/// Captures the result of a single benchmark scenario run, including all measured metrics,
/// environment information, and timing data for regression comparison.
/// </summary>
public sealed class BenchmarkResult
{
    /// <summary>
    /// The scenario name (e.g. "throughput", "latency", "scaling").
    /// </summary>
    public required string Scenario { get; init; }

    /// <summary>
    /// Human-readable description of what was measured.
    /// </summary>
    public required string Description { get; init; }

    /// <summary>
    /// When the benchmark was executed.
    /// </summary>
    public DateTimeOffset Timestamp { get; init; } = DateTimeOffset.UtcNow;

    /// <summary>
    /// Named metrics collected during the run (e.g. "throughput_msg_sec", "p99_ms").
    /// </summary>
    public Dictionary<string, double> Metrics { get; init; } = [];

    /// <summary>
    /// Environment information captured at run time.
    /// </summary>
    public EnvironmentInfo Environment { get; init; } = new();

    /// <summary>
    /// Wall-clock duration of the scenario.
    /// </summary>
    public TimeSpan Duration { get; init; }
}

/// <summary>
/// Captures machine and runtime environment details for reproducibility.
/// </summary>
public sealed class EnvironmentInfo
{
    /// <summary>Operating system version string.</summary>
    public string Os { get; init; } = System.Environment.OSVersion.ToString();

    /// <summary>Number of logical processors available.</summary>
    public int ProcessorCount { get; init; } = System.Environment.ProcessorCount;

    /// <summary>.NET runtime description.</summary>
    public string Runtime { get; init; } = RuntimeInformation.FrameworkDescription;

    /// <summary>Machine name.</summary>
    public string MachineName { get; init; } = System.Environment.MachineName;
}
