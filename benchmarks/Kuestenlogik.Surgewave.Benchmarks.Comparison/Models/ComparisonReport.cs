namespace Kuestenlogik.Surgewave.Benchmarks.Comparison.Models;

/// <summary>
/// Aggregates results from a single comparison scenario across multiple platforms.
/// Supports both the legacy Surgewave/Kafka pair and the new multi-platform model.
/// </summary>
public sealed class ComparisonReport
{
    /// <summary>Name of the comparison scenario.</summary>
    public required string ScenarioName { get; init; }

    /// <summary>Description of what the scenario measures.</summary>
    public required string Description { get; init; }

    /// <summary>All platform results for this scenario.</summary>
    public required List<ComparisonResult> Results { get; init; }

    /// <summary>Scenario-specific sub-results for multi-parameter scenarios (e.g. batch sizes, message sizes).</summary>
    public IReadOnlyList<ComparisonSubResult>? SubResults { get; init; }

    // ─── Legacy compatibility properties ──────────────────────────────────

    /// <summary>Surgewave Native result (first Surgewave embedded native result, or first result overall). For legacy compatibility.</summary>
    public ComparisonResult Surgewave => GetResult(BenchmarkPlatform.SurgewaveEmbeddedNative) ?? Results[0];

    /// <summary>Apache Kafka result (null if Kafka was unavailable or skipped). For legacy compatibility.</summary>
    public ComparisonResult? Kafka => GetResult(BenchmarkPlatform.ApacheKafkaContainer);

    // ─── Multi-platform accessors ─────────────────────────────────────────

    /// <summary>Find a result by platform type.</summary>
    public ComparisonResult? GetResult(BenchmarkPlatform platform) =>
        Results.Find(r => r.PlatformType == platform);

    /// <summary>Calculate produce throughput delta between two platforms (positive = comparison faster).</summary>
    public double? GetProduceThroughputDelta(BenchmarkPlatform baseline, BenchmarkPlatform comparison)
    {
        var baseResult = GetResult(baseline);
        var compResult = GetResult(comparison);
        if (baseResult is null || compResult is null || baseResult.ProduceThroughputMsgPerSec == 0)
            return null;
        return (compResult.ProduceThroughputMsgPerSec - baseResult.ProduceThroughputMsgPerSec)
            / baseResult.ProduceThroughputMsgPerSec * 100;
    }

    /// <summary>Calculate consume throughput delta between two platforms (positive = comparison faster).</summary>
    public double? GetConsumeThroughputDelta(BenchmarkPlatform baseline, BenchmarkPlatform comparison)
    {
        var baseResult = GetResult(baseline);
        var compResult = GetResult(comparison);
        if (baseResult is null || compResult is null || baseResult.ConsumeThroughputMsgPerSec == 0)
            return null;
        return (compResult.ConsumeThroughputMsgPerSec - baseResult.ConsumeThroughputMsgPerSec)
            / baseResult.ConsumeThroughputMsgPerSec * 100;
    }

    /// <summary>Produce throughput delta: Surgewave vs Kafka (positive = Surgewave faster). Legacy compatibility.</summary>
    public double? ProduceThroughputDeltaPercent => Kafka is null || Kafka.ProduceThroughputMsgPerSec == 0
        ? null
        : (Surgewave.ProduceThroughputMsgPerSec - Kafka.ProduceThroughputMsgPerSec) / Kafka.ProduceThroughputMsgPerSec * 100;

    /// <summary>Consume throughput delta: Surgewave vs Kafka (positive = Surgewave faster). Legacy compatibility.</summary>
    public double? ConsumeThroughputDeltaPercent => Kafka is null || Kafka.ConsumeThroughputMsgPerSec == 0
        ? null
        : (Surgewave.ConsumeThroughputMsgPerSec - Kafka.ConsumeThroughputMsgPerSec) / Kafka.ConsumeThroughputMsgPerSec * 100;

    /// <summary>Produce latency P99 delta: Surgewave vs Kafka (negative = Surgewave has lower latency). Legacy compatibility.</summary>
    public double? ProduceLatencyP99DeltaPercent => Kafka is null || Kafka.ProduceLatencyP99Ms == 0
        ? null
        : (Surgewave.ProduceLatencyP99Ms - Kafka.ProduceLatencyP99Ms) / Kafka.ProduceLatencyP99Ms * 100;

    /// <summary>Consume latency P99 delta: Surgewave vs Kafka (negative = Surgewave has lower latency). Legacy compatibility.</summary>
    public double? ConsumeLatencyP99DeltaPercent => Kafka is null || Kafka.ConsumeLatencyP99Ms == 0
        ? null
        : (Surgewave.ConsumeLatencyP99Ms - Kafka.ConsumeLatencyP99Ms) / Kafka.ConsumeLatencyP99Ms * 100;
}

/// <summary>
/// Sub-result for scenarios that sweep across a parameter (e.g. batch size, message size, producer count).
/// </summary>
public sealed class ComparisonSubResult
{
    /// <summary>Label for this parameter value (e.g. "Batch 100", "1KB", "3 producers").</summary>
    public required string Label { get; init; }

    /// <summary>All platform results for this sub-result.</summary>
    public required List<ComparisonResult> Results { get; init; }

    // ─── Legacy compatibility ─────────────────────────────────────────────

    /// <summary>Surgewave result for this parameter value. Legacy compatibility.</summary>
    public ComparisonResult Surgewave => Results.Find(r => r.PlatformType == BenchmarkPlatform.SurgewaveEmbeddedNative) ?? Results[0];

    /// <summary>Kafka result for this parameter value (null if skipped). Legacy compatibility.</summary>
    public ComparisonResult? Kafka => Results.Find(r => r.PlatformType == BenchmarkPlatform.ApacheKafkaContainer);
}
