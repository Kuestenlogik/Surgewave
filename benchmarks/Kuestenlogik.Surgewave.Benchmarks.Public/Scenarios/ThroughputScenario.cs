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
///
/// PHASE 1 SKELETON: the SUT-runs are stubbed to TODO. Phase 2
/// fills them in using the existing Comparison infrastructure
/// (Testcontainers Kafka + Redpanda, embedded Surgewave broker).
/// </summary>
public sealed class ThroughputScenario : IPublicScenario
{
    public string Id => "throughput-1p1c";
    public string Name => "Throughput — 1 producer → 1 consumer, single partition";
    public string Description =>
        "Single-producer raw throughput at the configured payload size, compression and ack-mode. " +
        "One topic, one partition, replication factor 1. Reports messages/sec + MB/sec. No latency.";

    public Task<IReadOnlyList<ScenarioResult>> RunAsync(
        PublicBenchmarkOptions options,
        CancellationToken cancellationToken)
    {
        // Phase 2: bring up Surgewave (embedded + Kafka-wire),
        // Apache Kafka + Redpanda via Testcontainers, run the
        // producer + consumer against each in turn, return one
        // ScenarioResult per system.
        IReadOnlyList<ScenarioResult> stub =
        [
            Stub("Surgewave Native", options),
            Stub("Surgewave Kafka-wire", options),
            Stub("Apache Kafka", options),
            Stub("Redpanda", options),
        ];
        return Task.FromResult(stub);
    }

    private static ScenarioResult Stub(string system, PublicBenchmarkOptions o) =>
        new(
            System: system,
            ThroughputMessagesPerSec: 0,
            ThroughputMegabytesPerSec: 0,
            P50LatencyMs: null,
            P90LatencyMs: null,
            P99LatencyMs: null,
            P999LatencyMs: null,
            P9999LatencyMs: null,
            MessagesSent: o.MessageCount,
            PayloadBytes: o.MessageCount * (long)o.PayloadBytes,
            WallClock: TimeSpan.Zero);
}
