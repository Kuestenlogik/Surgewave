using Kuestenlogik.Surgewave.Benchmarks.Public.Runners;
using Kuestenlogik.Surgewave.Benchmarks.Public.Sut;

namespace Kuestenlogik.Surgewave.Benchmarks.Public.Scenarios;

/// <summary>
/// Single-producer raw throughput against each broker. Measures
/// messages/sec + MB/sec at the configured payload size, with the
/// configured compression + ack-mode. No latency histogram — that
/// is what <see cref="LatencyScenario"/> is for.
///
/// Scenario is intentionally narrow: one producer, one topic, one
/// partition per broker, fixed-size messages. A "Surgewave is 5×
/// faster" claim needs to come from a setup that has obviously
/// nothing else going on — multi-producer / multi-topic adds
/// degrees of freedom that hide design wins or losses.
/// </summary>
public sealed class ThroughputScenario : IPublicScenario
{
    public string Id => "throughput-1p1c";
    public string Name => "Throughput — 1 producer → 1 consumer, single partition";
    public string Description =>
        "Single-producer raw throughput at the configured payload size, compression and ack-mode. " +
        "One topic, one partition, replication factor 1. Reports messages/sec + MB/sec. No latency.";

    public async Task<IReadOnlyList<ScenarioResult>> RunAsync(
        PublicBenchmarkOptions options,
        CancellationToken cancellationToken)
    {
        return await ScenarioPipeline.RunAcrossSutsAsync(
            options,
            cancellationToken,
            (sut, opts, ct) => ThroughputRunner.RunAsync(sut, opts, ct),
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
