using System.Diagnostics;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Client.Native.Operations.Performance;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Benchmarks.RealWorld.Scenarios;

/// <summary>
/// Measures end-to-end latency percentiles (P50, P90, P99, P99.9, P99.99) for produce
/// and consume operations on an embedded multi-broker cluster.
/// </summary>
public sealed class LatencyScenario
{
    private readonly BenchmarkConfig _config;

    public LatencyScenario(BenchmarkConfig config)
    {
        _config = config;
    }

    /// <summary>
    /// Produces messages one at a time, measuring individual produce latency,
    /// then consumes them measuring per-fetch latency and end-to-end round-trip time.
    /// </summary>
    public async Task<BenchmarkResult> RunAsync()
    {
        AnsiConsole.MarkupLine("[bold cyan]Latency Scenario[/]");
        AnsiConsole.MarkupLine($"  Brokers: {_config.BrokerCount}, Messages: {_config.MessageCount:N0}, Size: {_config.MessageSizeBytes}B");

        var overallSw = Stopwatch.StartNew();
        var metrics = new Dictionary<string, double>();

        await using var cluster = await ClusterSetup.CreateAsync(
            _config.BrokerCount,
            StorageEngines.Memory,
            partitions: 1);

        var topicName = "bench-latency";
        await cluster.CreateTopicAsync(topicName, 1);

        var messageValue = new byte[_config.MessageSizeBytes];
        Random.Shared.NextBytes(messageValue);

        await using var client = await cluster.GetClientAsync(0);

        // --- Warmup ---
        var warmupCount = Math.Min(1000, _config.MessageCount / 10);
        AnsiConsole.MarkupLine($"  [dim]Warming up ({warmupCount} messages)...[/]");

        await using (var warmupProducer = new SurgewaveBatchingProducer(client, topicName + "-warmup", 0, maxBatchSize: 100, lingerTime: TimeSpan.FromMilliseconds(1)))
        {
            for (int i = 0; i < warmupCount; i++)
                await warmupProducer.ProduceAsync(null, messageValue);
            await warmupProducer.FlushAsync();
        }

        // --- Produce latency (unbatched, single message at a time) ---
        AnsiConsole.MarkupLine("  [dim]Measuring produce latency...[/]");
        var produceHistogram = new LatencyHistogram();
        var produceTopicName = topicName + "-produce";

        await using (var singleProducer = new SurgewaveBatchingProducer(client, produceTopicName, 0, maxBatchSize: 1, lingerTime: TimeSpan.FromMilliseconds(0)))
        {
            for (int i = 0; i < _config.MessageCount; i++)
            {
                var sw = Stopwatch.StartNew();
                await singleProducer.ProduceAsync(null, messageValue);
                await singleProducer.FlushAsync();
                sw.Stop();

                produceHistogram.RecordMicroseconds(sw.Elapsed.TotalMicroseconds);
            }
        }

        produceHistogram.PopulateMetrics(metrics, "produce_");
        AnsiConsole.MarkupLine($"  [green]Produce P50={metrics["produce_p50_ms"]:F3}ms, P99={metrics["produce_p99_ms"]:F3}ms, P99.9={metrics["produce_p99_9_ms"]:F3}ms[/]");

        // --- Consume latency (per-fetch) ---
        AnsiConsole.MarkupLine("  [dim]Measuring consume latency...[/]");
        var consumeHistogram = new LatencyHistogram();
        long offset = 0;
        var consumed = 0;

        while (consumed < _config.MessageCount)
        {
            var sw = Stopwatch.StartNew();
            var result = await client.Messaging.ReceiveAsync(produceTopicName, 0, offset, 64 * 1024);
            sw.Stop();

            if (result.Messages.Count == 0)
            {
                await Task.Delay(5);
                continue;
            }

            consumeHistogram.RecordMicroseconds(sw.Elapsed.TotalMicroseconds);
            consumed += result.Messages.Count;
            offset = result.Messages[^1].Offset + 1;
        }

        consumeHistogram.PopulateMetrics(metrics, "consume_");
        AnsiConsole.MarkupLine($"  [green]Consume P50={metrics["consume_p50_ms"]:F3}ms, P99={metrics["consume_p99_ms"]:F3}ms, P99.9={metrics["consume_p99_9_ms"]:F3}ms[/]");

        // --- End-to-end latency (produce then immediately fetch) ---
        AnsiConsole.MarkupLine("  [dim]Measuring end-to-end latency...[/]");
        var e2eHistogram = new LatencyHistogram();
        var e2eCount = Math.Min(_config.MessageCount, 5000);
        var e2eTopicName = topicName + "-e2e";
        long e2eOffset = 0;

        await using (var e2eProducer = new SurgewaveBatchingProducer(client, e2eTopicName, 0, maxBatchSize: 1, lingerTime: TimeSpan.FromMilliseconds(0)))
        {
            for (int i = 0; i < e2eCount; i++)
            {
                var sw = Stopwatch.StartNew();
                await e2eProducer.ProduceAsync(null, messageValue);
                await e2eProducer.FlushAsync();

                // Immediately fetch
                while (true)
                {
                    var result = await client.Messaging.ReceiveAsync(e2eTopicName, 0, e2eOffset, 64 * 1024);
                    if (result.Messages.Count > 0)
                    {
                        e2eOffset = result.Messages[^1].Offset + 1;
                        break;
                    }

                    await Task.Delay(1);
                }

                sw.Stop();
                e2eHistogram.RecordMicroseconds(sw.Elapsed.TotalMicroseconds);
            }
        }

        e2eHistogram.PopulateMetrics(metrics, "e2e_");
        AnsiConsole.MarkupLine($"  [green]E2E P50={metrics["e2e_p50_ms"]:F3}ms, P99={metrics["e2e_p99_ms"]:F3}ms, P99.9={metrics["e2e_p99_9_ms"]:F3}ms[/]");

        overallSw.Stop();

        return new BenchmarkResult
        {
            Scenario = "latency",
            Description = $"End-to-end latency percentiles with {_config.BrokerCount} broker(s), {_config.MessageCount:N0} messages x {_config.MessageSizeBytes}B",
            Metrics = metrics,
            Duration = overallSw.Elapsed
        };
    }
}
