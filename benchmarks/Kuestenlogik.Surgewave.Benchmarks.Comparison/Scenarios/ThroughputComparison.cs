using Kuestenlogik.Surgewave.Benchmarks.Comparison.Models;

namespace Kuestenlogik.Surgewave.Benchmarks.Comparison.Scenarios;

/// <summary>
/// Compares raw throughput: produce 100K messages (100B each, batch 1000) across all enabled platforms.
/// Measures messages/sec and MB/sec for both produce and consume operations.
/// </summary>
public sealed class ThroughputComparison : ComparisonScenario
{
    public override string Name => "Throughput";

    public override string Description =>
        "Batched produce/consume throughput comparison (msg/sec, MB/sec)";

    public override async Task<ComparisonResult> RunSurgewaveAsync(BenchmarkParams p, CancellationToken ct)
    {
        return await RunSurgewaveThroughputAsync(p.MessageCount, p.MessageSizeBytes, p.BatchSize, ct: ct);
    }

    public override async Task<ComparisonResult> RunKafkaAsync(BenchmarkParams p, CancellationToken ct)
    {
        var bootstrap = await GetKafkaBootstrapAsync(p);
        return await RunKafkaThroughputAsync(bootstrap, p.MessageCount, p.MessageSizeBytes, p.BatchSize, ct: ct);
    }
}
