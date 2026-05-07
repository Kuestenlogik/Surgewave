namespace Kuestenlogik.Surgewave.Benchmarks.Comparison.Models;

/// <summary>
/// Captures the performance metrics from a single platform run within a comparison scenario.
/// Each scenario produces one result per platform tested.
/// </summary>
public sealed class ComparisonResult
{
    /// <summary>Platform identifier (e.g. "Surgewave Native", "Surgewave Kafka Protocol", "Apache Kafka").</summary>
    public required string Platform { get; init; }

    /// <summary>Typed platform enum for programmatic access.</summary>
    public BenchmarkPlatform PlatformType { get; init; }

    /// <summary>Producer throughput in messages per second.</summary>
    public double ProduceThroughputMsgPerSec { get; init; }

    /// <summary>Producer throughput in megabytes per second.</summary>
    public double ProduceThroughputMbPerSec { get; init; }

    /// <summary>Consumer throughput in messages per second.</summary>
    public double ConsumeThroughputMsgPerSec { get; init; }

    /// <summary>Consumer throughput in megabytes per second.</summary>
    public double ConsumeThroughputMbPerSec { get; init; }

    /// <summary>Producer latency P50 in milliseconds.</summary>
    public double ProduceLatencyP50Ms { get; init; }

    /// <summary>Producer latency P90 in milliseconds.</summary>
    public double ProduceLatencyP90Ms { get; init; }

    /// <summary>Producer latency P99 in milliseconds.</summary>
    public double ProduceLatencyP99Ms { get; init; }

    /// <summary>Consumer latency P50 in milliseconds.</summary>
    public double ConsumeLatencyP50Ms { get; init; }

    /// <summary>Consumer latency P90 in milliseconds.</summary>
    public double ConsumeLatencyP90Ms { get; init; }

    /// <summary>Consumer latency P99 in milliseconds.</summary>
    public double ConsumeLatencyP99Ms { get; init; }

    /// <summary>Total bytes produced during the test.</summary>
    public long TotalBytesProduced { get; init; }

    /// <summary>Wall-clock duration of the test run.</summary>
    public TimeSpan Duration { get; init; }
}
