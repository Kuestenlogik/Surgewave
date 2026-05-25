using System.Diagnostics;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Client.Native.Operations.Performance;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Benchmarks.RealWorld.Scenarios;

/// <summary>
/// Verifies linear scaling by running the same workload on clusters of increasing size
/// (1, 2, 3, 5 brokers) and reporting throughput at each size along with the scaling factor.
/// </summary>
public sealed class ScalingScenario
{
    private readonly BenchmarkConfig _config;

    public ScalingScenario(BenchmarkConfig config)
    {
        _config = config;
    }

    /// <summary>
    /// Runs the same produce workload on clusters of 1, 2, 3, and 5 brokers.
    /// Reports throughput and scaling factor compared to the single-broker baseline.
    /// </summary>
    public async Task<BenchmarkResult> RunAsync()
    {
        var clusterSizes = new[] { 1, 2, 3, 5 };
        AnsiConsole.MarkupLine("[bold cyan]Scaling Scenario[/]");
        AnsiConsole.MarkupLine($"  Cluster sizes: {string.Join(", ", clusterSizes)}, Messages: {_config.MessageCount:N0}, Size: {_config.MessageSizeBytes}B");

        var overallSw = Stopwatch.StartNew();
        var metrics = new Dictionary<string, double>();
        var baselineThroughput = 0.0;

        var messageValue = new byte[_config.MessageSizeBytes];
        Random.Shared.NextBytes(messageValue);

        foreach (var size in clusterSizes)
        {
            AnsiConsole.MarkupLine($"  [dim]Testing {size}-broker cluster...[/]");

            var throughput = await RunClusterBenchmarkAsync(size, messageValue);
            var durationMs = throughput.DurationMs;
            var msgPerSec = throughput.MsgPerSec;

            if (size == 1)
                baselineThroughput = msgPerSec;

            var scalingFactor = baselineThroughput > 0 ? msgPerSec / baselineThroughput : 1.0;

            metrics[$"brokers_{size}_throughput_msg_sec"] = msgPerSec;
            metrics[$"brokers_{size}_scaling_factor"] = scalingFactor;
            metrics[$"brokers_{size}_duration_ms"] = durationMs;

            AnsiConsole.MarkupLine(
                $"  [green]{size} broker(s): {msgPerSec:N0} msg/sec (scaling factor: {scalingFactor:F2}x)[/]");
        }

        overallSw.Stop();

        return new BenchmarkResult
        {
            Scenario = "scaling",
            Description = $"Linear scaling verification across {string.Join(", ", clusterSizes)} broker(s), {_config.MessageCount:N0} messages x {_config.MessageSizeBytes}B",
            Metrics = metrics,
            Duration = overallSw.Elapsed
        };
    }

    private async Task<(double MsgPerSec, double DurationMs)> RunClusterBenchmarkAsync(int clusterSize, byte[] messageValue)
    {
        await using var cluster = await ClusterSetup.CreateAsync(clusterSize, StorageEngines.Memory, partitions: 1);

        var topicName = $"bench-scaling-{clusterSize}";
        await cluster.CreateTopicAsync(topicName, 1);

        await using var client = await cluster.GetClientAsync(0);
        await using var producer = new SurgewaveBatchingProducer(
            client,
            topicName,
            partition: 0,
            maxBatchSize: _config.BatchSize,
            lingerTime: TimeSpan.FromMilliseconds(5));

        var sw = Stopwatch.StartNew();
        for (int i = 0; i < _config.MessageCount; i++)
        {
            await producer.ProduceAsync(null, messageValue);
        }

        await producer.FlushAsync();
        sw.Stop();

        var durationMs = Math.Max(1, sw.ElapsedMilliseconds);
        var msgPerSec = _config.MessageCount * 1000.0 / durationMs;

        return (msgPerSec, durationMs);
    }
}
