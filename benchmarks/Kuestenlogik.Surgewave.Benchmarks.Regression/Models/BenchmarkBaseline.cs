namespace Kuestenlogik.Surgewave.Benchmarks.Regression.Models;

/// <summary>
/// Represents a complete benchmark baseline containing all benchmark entries
/// and metadata about the environment where measurements were taken.
/// </summary>
public sealed class BenchmarkBaseline
{
    /// <summary>Schema version for forward compatibility.</summary>
    public int Version { get; set; } = 1;

    /// <summary>When this baseline was created or last updated.</summary>
    public DateTimeOffset Timestamp { get; set; } = DateTimeOffset.UtcNow;

    /// <summary>Environment metadata for the baseline measurements.</summary>
    public EnvironmentInfo? Environment { get; set; }

    /// <summary>Benchmark entries keyed by fully-qualified benchmark name.</summary>
    public Dictionary<string, BenchmarkEntry> Benchmarks { get; set; } = [];
}

/// <summary>
/// Describes the hardware and runtime environment where benchmarks were executed.
/// </summary>
public sealed class EnvironmentInfo
{
    /// <summary>Operating system description.</summary>
    public string? Os { get; set; }

    /// <summary>CPU model description.</summary>
    public string? Cpu { get; set; }

    /// <summary>Runtime version (e.g., ".NET 10").</summary>
    public string? Runtime { get; set; }
}
