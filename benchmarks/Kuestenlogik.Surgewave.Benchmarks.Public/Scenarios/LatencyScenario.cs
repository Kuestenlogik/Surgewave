using Kuestenlogik.Surgewave.Benchmarks.Public.Runners;
using Kuestenlogik.Surgewave.Benchmarks.Public.Sut;

namespace Kuestenlogik.Surgewave.Benchmarks.Public.Scenarios;

/// <summary>
/// End-to-end produce-to-consume latency, percentile-reported via
/// HdrHistogram. Each measurement is the timestamp delta between
/// producer-send-completion and consumer-receive at acks=all,
/// recorded at microsecond resolution so the P99.99 row is
/// meaningful (BenchmarkDotNet's mean/StdDev would not be).
/// </summary>
public sealed class LatencyScenario : IPublicScenario
{
    public string Id => "latency-acks-all";
    public string Name => "Latency — produce→consume P50/P90/P99/P99.9/P99.99 (acks=all)";
    public string Description =>
        "End-to-end produce-to-consume latency under steady-state moderate load (acks=all, replication-factor 1). " +
        "Recorded via HdrHistogram at microsecond resolution. The P99.99 row is the operationally important one — " +
        "the long-tail latency operators have to plan their consumer-lag budget against.";

    public async Task<IReadOnlyList<ScenarioResult>> RunAsync(
        PublicBenchmarkOptions options,
        CancellationToken cancellationToken)
    {
        return await ScenarioPipeline.RunAcrossSutsAsync(
            options,
            cancellationToken,
            (sut, opts, ct) => LatencyRunner.RunAsync(sut, opts, ct),
            placeholderForSkipped: name => new ScenarioResult(
                System: name + " (skipped — container bringup failed)",
                ThroughputMessagesPerSec: 0,
                ThroughputMegabytesPerSec: 0,
                P50LatencyMs: null,
                P90LatencyMs: null,
                P99LatencyMs: null,
                P999LatencyMs: null,
                P9999LatencyMs: null,
                MessagesSent: 0,
                PayloadBytes: 0,
                WallClock: TimeSpan.Zero))
            .ConfigureAwait(false);
    }
}
