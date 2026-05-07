using System.Diagnostics;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Client.Native.Operations.Performance;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Benchmarks.RealWorld.Scenarios;

/// <summary>
/// Compares throughput and latency across different storage engines
/// (Memory, File, Arrow) using the same workload on single-broker clusters.
/// </summary>
public sealed class StorageScenario
{
    private readonly BenchmarkConfig _config;

    public StorageScenario(BenchmarkConfig config)
    {
        _config = config;
    }

    /// <summary>
    /// Runs the same produce/consume workload on Memory, File, and Arrow storage engines.
    /// Reports throughput comparison and highlights the fastest engine.
    /// </summary>
    public async Task<BenchmarkResult> RunAsync()
    {
        var storageModes = new[]
        {
            (Engine: StorageEngines.Memory, Name: "memory"),
            (Engine: StorageEngines.File, Name: "file"),
            (Engine: "arrow", Name: "arrow")
        };

        AnsiConsole.MarkupLine("[bold cyan]Storage Scenario[/]");
        AnsiConsole.MarkupLine($"  Engines: {string.Join(", ", storageModes.Select(s => s.Name))}, Messages: {_config.MessageCount:N0}, Size: {_config.MessageSizeBytes}B");

        var overallSw = Stopwatch.StartNew();
        var metrics = new Dictionary<string, double>();

        var messageValue = new byte[_config.MessageSizeBytes];
        Random.Shared.NextBytes(messageValue);

        foreach (var (engine, name) in storageModes)
        {
            AnsiConsole.MarkupLine($"  [dim]Testing {name} storage...[/]");

            var result = await RunStorageBenchmarkAsync(engine, name, messageValue);

            metrics[$"{name}_produce_msg_sec"] = result.ProduceMsgPerSec;
            metrics[$"{name}_produce_mb_sec"] = result.ProduceMBPerSec;
            metrics[$"{name}_consume_msg_sec"] = result.ConsumeMsgPerSec;
            metrics[$"{name}_consume_mb_sec"] = result.ConsumeMBPerSec;
            metrics[$"{name}_produce_duration_ms"] = result.ProduceDurationMs;
            metrics[$"{name}_consume_duration_ms"] = result.ConsumeDurationMs;

            AnsiConsole.MarkupLine($"  [green]{name}: Produce={result.ProduceMsgPerSec:N0} msg/sec ({result.ProduceMBPerSec:N1} MB/s), Consume={result.ConsumeMsgPerSec:N0} msg/sec ({result.ConsumeMBPerSec:N1} MB/s)[/]");
        }

        // Compute speedups relative to file storage
        if (metrics.TryGetValue("file_produce_msg_sec", out var fileProduceThroughput) && fileProduceThroughput > 0)
        {
            foreach (var (_, name) in storageModes.Where(s => s.Name != "file"))
            {
                if (metrics.TryGetValue($"{name}_produce_msg_sec", out var t))
                    metrics[$"{name}_vs_file_produce_speedup"] = t / fileProduceThroughput;
            }
        }

        if (metrics.TryGetValue("file_consume_msg_sec", out var fileConsumeThroughput) && fileConsumeThroughput > 0)
        {
            foreach (var (_, name) in storageModes.Where(s => s.Name != "file"))
            {
                if (metrics.TryGetValue($"{name}_consume_msg_sec", out var t))
                    metrics[$"{name}_vs_file_consume_speedup"] = t / fileConsumeThroughput;
            }
        }

        overallSw.Stop();

        return new BenchmarkResult
        {
            Scenario = "storage",
            Description = $"Storage engine comparison (Memory/File/Arrow), {_config.MessageCount:N0} messages x {_config.MessageSizeBytes}B, batch={_config.BatchSize}",
            Metrics = metrics,
            Duration = overallSw.Elapsed
        };
    }

    private async Task<(double ProduceMsgPerSec, double ProduceMBPerSec, double ConsumeMsgPerSec, double ConsumeMBPerSec, double ProduceDurationMs, double ConsumeDurationMs)> RunStorageBenchmarkAsync(
        string storageEngine, string name, byte[] messageValue)
    {
        await using var cluster = await ClusterSetup.CreateAsync(1, storageEngine, partitions: 1);

        var topicName = $"bench-storage-{name}";
        await cluster.CreateTopicAsync(topicName, 1);

        await using var client = await cluster.GetClientAsync(0);

        // --- Produce ---
        await using var producer = new SurgewaveBatchingProducer(
            client,
            topicName,
            partition: 0,
            maxBatchSize: _config.BatchSize,
            lingerTime: TimeSpan.FromMilliseconds(5));

        var produceSw = Stopwatch.StartNew();
        for (int i = 0; i < _config.MessageCount; i++)
        {
            await producer.ProduceAsync(null, messageValue);
        }

        await producer.FlushAsync();
        produceSw.Stop();

        var produceMs = Math.Max(1, produceSw.ElapsedMilliseconds);
        var produceThroughput = _config.MessageCount * 1000.0 / produceMs;
        var produceMBSec = (long)_config.MessageCount * _config.MessageSizeBytes / 1024.0 / 1024.0 * 1000.0 / produceMs;

        // --- Consume ---
        var consumeSw = Stopwatch.StartNew();
        var consumed = 0;
        long offset = 0;

        while (consumed < _config.MessageCount)
        {
            var result = await client.Messaging.ReceiveAsync(topicName, 0, offset, 1024 * 1024);
            if (result.Messages.Count == 0)
            {
                await Task.Delay(10);
                if (consumeSw.ElapsedMilliseconds > _config.DurationSeconds * 1000)
                    break;
                continue;
            }

            consumed += result.Messages.Count;
            offset = result.Messages[^1].Offset + 1;
        }

        consumeSw.Stop();

        var consumeMs = Math.Max(1, consumeSw.ElapsedMilliseconds);
        var consumeThroughput = consumed * 1000.0 / consumeMs;
        var consumeMBSec = (long)consumed * _config.MessageSizeBytes / 1024.0 / 1024.0 * 1000.0 / consumeMs;

        return (produceThroughput, produceMBSec, consumeThroughput, consumeMBSec, produceMs, consumeMs);
    }
}
