using Kuestenlogik.Surgewave.Transport;

namespace Kuestenlogik.Surgewave.Benchmarks.QuicVsTcp;

/// <summary>
/// One row in the benchmark matrix: transport, loss rate, one-way latency.
/// </summary>
public sealed record BenchmarkScenario(
    string Name,
    SurgewaveTransportType Transport,
    double DropRate,
    int LatencyMs);

/// <summary>
/// Aggregated results for a single scenario run.
/// </summary>
public sealed record BenchmarkResult(
    BenchmarkScenario Scenario,
    int MessagesSent,
    int MessageSize,
    TimeSpan Elapsed,
    long ProxyDatagramsDropped,
    long ProxyDatagramsForwarded,
    string? Error)
{
    public double MessagesPerSecond => MessagesSent / Math.Max(Elapsed.TotalSeconds, 1e-9);
    public double MegabytesPerSecond => MessagesPerSecond * MessageSize / (1024.0 * 1024.0);
}
