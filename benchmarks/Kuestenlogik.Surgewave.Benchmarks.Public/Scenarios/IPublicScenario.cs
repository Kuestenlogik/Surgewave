namespace Kuestenlogik.Surgewave.Benchmarks.Public.Scenarios;

/// <summary>
/// Contract that every G3 public benchmark scenario implements. The
/// runner only sees this interface — the concrete scenarios
/// (Throughput, Latency, Storage…) plug in by registration. Keeps
/// the runner free of scenario-specific knowledge so adding a new
/// scenario is one file plus a registration line.
///
/// Each scenario MUST be deterministic in its setup: same message
/// count, same payload size, same record-batch policy, same
/// compression. Anything operator-tweakable for the comparison
/// lives in <see cref="PublicBenchmarkOptions"/> and is recorded in
/// the result file alongside the numbers so the run is reproducible
/// from disk.
/// </summary>
public interface IPublicScenario
{
    /// <summary>Stable identifier — used as JSON key + Markdown anchor.</summary>
    string Id { get; }

    /// <summary>Human-readable name shown in the Markdown report header.</summary>
    string Name { get; }

    /// <summary>One-paragraph what-this-measures-and-why blurb for the report.</summary>
    string Description { get; }

    /// <summary>
    /// Run the scenario across all configured systems (Surgewave Native,
    /// Surgewave Kafka-wire, Apache Kafka, Redpanda) and return one
    /// result entry per system. The runner is responsible for ensuring
    /// containers are up and torn down between scenarios.
    /// </summary>
    Task<IReadOnlyList<ScenarioResult>> RunAsync(
        PublicBenchmarkOptions options,
        CancellationToken cancellationToken);
}

/// <summary>
/// One row in the result table — what system, what we measured, what
/// the numbers were. Percentile fields are nullable because not every
/// scenario emits a latency histogram (throughput scenarios leave them
/// null and the report shows them as em-dash).
/// </summary>
public sealed record ScenarioResult(
    string System,
    double ThroughputMessagesPerSec,
    double ThroughputMegabytesPerSec,
    double? P50LatencyMs,
    double? P90LatencyMs,
    double? P99LatencyMs,
    double? P999LatencyMs,
    double? P9999LatencyMs,
    long MessagesSent,
    long PayloadBytes,
    TimeSpan WallClock);

/// <summary>
/// Operator-tweakable inputs that apply to all scenarios in a run.
/// These are recorded in the result file so two reports are only
/// directly comparable if their options match.
/// </summary>
public sealed record PublicBenchmarkOptions(
    int MessageCount,
    int PayloadBytes,
    int BatchSize,
    string CompressionCodec,
    int Acks,
    int ReplicationFactor,
    int WarmupRounds,
    int MeasurementRounds);
