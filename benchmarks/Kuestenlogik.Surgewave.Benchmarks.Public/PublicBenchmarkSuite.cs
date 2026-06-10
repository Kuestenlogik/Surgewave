using Kuestenlogik.Surgewave.Benchmarks.Public.Scenarios;

namespace Kuestenlogik.Surgewave.Benchmarks.Public;

/// <summary>
/// Curated set of G3 scenarios — what gets run by
/// <c>surgewave-bench public</c>. Deliberately small so the output
/// report stays readable and the run completes in &lt; 3 h on the
/// reference hardware (AWS c7i.4xlarge, see
/// <c>infra/aws-bench/COSTS.md</c>).
///
/// Anything beyond this list (microbenchmarks, storage-engine
/// internals, SIMD) lives in the other Benchmarks.* projects and is
/// dev-iteration territory — not part of the public-claim surface.
/// </summary>
public static class PublicBenchmarkSuite
{
    public static IReadOnlyList<IPublicScenario> AllScenarios =>
    [
        new ThroughputScenario(),
        new LatencyScenario(),
        // Phase 2 adds: TailLatencyUnderProducerStallScenario,
        // MultiPartitionFanoutScenario, ConsumerLagRecoveryScenario.
    ];
}
