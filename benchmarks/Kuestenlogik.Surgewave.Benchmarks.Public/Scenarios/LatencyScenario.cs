using HdrHistogram;

namespace Kuestenlogik.Surgewave.Benchmarks.Public.Scenarios;

/// <summary>
/// End-to-end produce-to-consume latency, percentile-reported via
/// HdrHistogram. Each measurement is the timestamp delta between
/// producer-send-completion and consumer-receive at acks=all,
/// recorded at microsecond resolution so the P99.99 row is
/// meaningful (BenchmarkDotNet's mean/StdDev would not be).
///
/// PHASE 1 SKELETON: the histogram-collection loop is stubbed.
/// Phase 2 wires the four SUTs (Surgewave Native, Surgewave Kafka-
/// wire, Apache Kafka, Redpanda) into the producer-consumer
/// pipeline and reports the histogram percentiles back as
/// <see cref="ScenarioResult"/> P50/P90/P99/P999/P9999 fields.
/// </summary>
public sealed class LatencyScenario : IPublicScenario
{
    public string Id => "latency-acks-all";
    public string Name => "Latency — produce→consume P50/P90/P99/P99.9/P99.99 (acks=all)";
    public string Description =>
        "End-to-end produce-to-consume latency under steady-state moderate load (acks=all, replication-factor 1). " +
        "Recorded via HdrHistogram at microsecond resolution. The P99.99 row is the operationally important one — " +
        "the long-tail latency operators have to plan their consumer-lag budget against.";

    public Task<IReadOnlyList<ScenarioResult>> RunAsync(
        PublicBenchmarkOptions options,
        CancellationToken cancellationToken)
    {
        // Phase 2: per SUT bring up the broker, warmup, then run the
        // measurement phase recording each end-to-end latency in a
        // HdrHistogram (max value 60s, 3 significant digits). The
        // example below shows the shape — the actual capture loop is
        // wired in Phase 2.
        IReadOnlyList<ScenarioResult> stub =
        [
            BuildResultFromStubHistogram("Surgewave Native", options),
            BuildResultFromStubHistogram("Surgewave Kafka-wire", options),
            BuildResultFromStubHistogram("Apache Kafka", options),
            BuildResultFromStubHistogram("Redpanda", options),
        ];
        return Task.FromResult(stub);
    }

    private static ScenarioResult BuildResultFromStubHistogram(string system, PublicBenchmarkOptions o)
    {
        // Create an empty histogram with the resolution we want to use
        // in Phase 2. Recording a single sample so the percentile calls
        // don't divide-by-zero — Phase 2 replaces this with the real
        // recording loop.
        var histogram = new LongHistogram(
            highestTrackableValue: TimeStamp.Seconds(60),
            numberOfSignificantValueDigits: 3);
        histogram.RecordValue(1);

        static double ToMillis(LongHistogram h, double p) => h.GetValueAtPercentile(p) / 1_000.0;

        return new ScenarioResult(
            System: system,
            ThroughputMessagesPerSec: 0,
            ThroughputMegabytesPerSec: 0,
            P50LatencyMs: ToMillis(histogram, 50),
            P90LatencyMs: ToMillis(histogram, 90),
            P99LatencyMs: ToMillis(histogram, 99),
            P999LatencyMs: ToMillis(histogram, 99.9),
            P9999LatencyMs: ToMillis(histogram, 99.99),
            MessagesSent: o.MessageCount,
            PayloadBytes: o.MessageCount * (long)o.PayloadBytes,
            WallClock: TimeSpan.Zero);
    }
}
