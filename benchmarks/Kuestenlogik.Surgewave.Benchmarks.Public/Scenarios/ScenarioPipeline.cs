using Kuestenlogik.Surgewave.Benchmarks.Public.Sut;

namespace Kuestenlogik.Surgewave.Benchmarks.Public.Scenarios;

/// <summary>
/// Shared "for each SUT: bring up → run → tear down" loop. Centralises
/// the SUT lifecycle so each scenario file is just the measurement
/// callback. Failures on Kafka/Redpanda bringup (Docker missing,
/// image pull failed, …) record a skipped row rather than aborting
/// the whole report.
/// </summary>
internal static class ScenarioPipeline
{
    private static readonly SutKind[] SutOrder =
    [
        SutKind.SurgewaveNative,
        SutKind.SurgewaveKafkaWire,
        SutKind.ApacheKafka,
        SutKind.Redpanda,
    ];

    public static async Task<IReadOnlyList<ScenarioResult>> RunAcrossSutsAsync(
        PublicBenchmarkOptions options,
        CancellationToken cancellationToken,
        Func<IBrokerSut, PublicBenchmarkOptions, CancellationToken, Task<ScenarioResult>> measure,
        Func<string, ScenarioResult> placeholderForSkipped)
    {
        var results = new List<ScenarioResult>(SutOrder.Length);
        foreach (var kind in SutOrder)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var sut = await SutFactory.TryStartAsync(kind, cancellationToken).ConfigureAwait(false);
            if (sut is null)
            {
                results.Add(placeholderForSkipped(KindToDisplayName(kind)));
                continue;
            }

            try
            {
                if (options.WarmupRounds > 0)
                {
                    var warmupOptions = options with
                    {
                        MessageCount = Math.Min(options.MessageCount, Math.Max(1_000, options.MessageCount / 10)),
                    };
                    for (var w = 0; w < options.WarmupRounds; w++)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        _ = await measure(sut, warmupOptions, cancellationToken).ConfigureAwait(false);
                    }
                }

                var rounds = Math.Max(1, options.MeasurementRounds);
                ScenarioResult? best = null;
                for (var r = 0; r < rounds; r++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var result = await measure(sut, options, cancellationToken).ConfigureAwait(false);
                    best = best is null ? result : BetterOf(best, result);
                }
                results.Add(best!);
            }
            finally
            {
                await sut.DisposeAsync().ConfigureAwait(false);
            }
        }
        return results;
    }

    private static string KindToDisplayName(SutKind kind) => kind switch
    {
        SutKind.SurgewaveNative => "Surgewave Native",
        SutKind.SurgewaveKafkaWire => "Surgewave Kafka-wire",
        SutKind.ApacheKafka => "Apache Kafka",
        SutKind.Redpanda => "Redpanda",
        _ => kind.ToString(),
    };

    /// <summary>
    /// Pick the "better" of two runs. For throughput runs (no latency
    /// fields) the higher msg/s wins. For latency runs the lower P99
    /// wins. Mixed: throughput tie-breaks by lower P99.
    /// </summary>
    private static ScenarioResult BetterOf(ScenarioResult a, ScenarioResult b)
    {
        var aHasLatency = a.P99LatencyMs is not null;
        var bHasLatency = b.P99LatencyMs is not null;
        if (aHasLatency && bHasLatency)
        {
            return a.P99LatencyMs <= b.P99LatencyMs ? a : b;
        }
        return a.ThroughputMessagesPerSec >= b.ThroughputMessagesPerSec ? a : b;
    }
}
