namespace Kuestenlogik.Surgewave.Benchmarks.RealWorld;

/// <summary>
/// Configuration for a real-world benchmark run. Controls cluster size, message volume,
/// message size, duration limits, and output paths.
/// </summary>
public sealed class BenchmarkConfig
{
    /// <summary>Number of brokers in the embedded cluster.</summary>
    public int BrokerCount { get; set; } = 3;

    /// <summary>Number of messages to produce per test.</summary>
    public int MessageCount { get; set; } = 100_000;

    /// <summary>Size of each message payload in bytes.</summary>
    public int MessageSizeBytes { get; set; } = 100;

    /// <summary>Maximum duration for a single scenario in seconds.</summary>
    public int DurationSeconds { get; set; } = 60;

    /// <summary>Path to save JSON results.</summary>
    public string? OutputPath { get; set; }

    /// <summary>Path to a baseline JSON file for regression comparison.</summary>
    public string? ComparePath { get; set; }

    /// <summary>Path to generate a Markdown report.</summary>
    public string? ReportPath { get; set; }

    /// <summary>Number of partitions per topic.</summary>
    public int Partitions { get; set; } = 3;

    /// <summary>Batch size for batching producer.</summary>
    public int BatchSize { get; set; } = 1000;
}
