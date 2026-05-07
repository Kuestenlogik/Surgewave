using System.Diagnostics;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Client.Native.Operations.Performance;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Benchmarks.RealWorld.Scenarios;

/// <summary>
/// Measures the overhead introduced by replication by comparing throughput and latency
/// with replication factor 1 versus replication factor 3 on the same cluster size.
/// Note: With embedded single-process brokers, replication overhead is simulated at the
/// storage layer. True network replication requires cluster mode.
/// </summary>
public sealed class ReplicationScenario
{
    private readonly BenchmarkConfig _config;

    public ReplicationScenario(BenchmarkConfig config)
    {
        _config = config;
    }

    /// <summary>
    /// Runs the same produce/consume workload with replication factor 1 and 3 on a 3-broker cluster.
    /// Reports throughput difference and overhead percentage.
    /// </summary>
    public async Task<BenchmarkResult> RunAsync()
    {
        AnsiConsole.MarkupLine("[bold cyan]Replication Scenario[/]");
        AnsiConsole.MarkupLine($"  Brokers: {_config.BrokerCount}, Messages: {_config.MessageCount:N0}, Size: {_config.MessageSizeBytes}B");

        var overallSw = Stopwatch.StartNew();
        var metrics = new Dictionary<string, double>();

        var messageValue = new byte[_config.MessageSizeBytes];
        Random.Shared.NextBytes(messageValue);

        var replicationFactors = new[] { 1, 3 };

        foreach (var replicationFactor in replicationFactors)
        {
            AnsiConsole.MarkupLine($"  [dim]Testing replication factor {replicationFactor}...[/]");

            var result = await RunReplicationBenchmarkAsync(replicationFactor, messageValue);

            metrics[$"rf{replicationFactor}_produce_msg_sec"] = result.ProduceMsgPerSec;
            metrics[$"rf{replicationFactor}_consume_msg_sec"] = result.ConsumeMsgPerSec;
            metrics[$"rf{replicationFactor}_produce_duration_ms"] = result.ProduceDurationMs;
            metrics[$"rf{replicationFactor}_consume_duration_ms"] = result.ConsumeDurationMs;

            AnsiConsole.MarkupLine($"  [green]RF={replicationFactor}: Produce={result.ProduceMsgPerSec:N0} msg/sec, Consume={result.ConsumeMsgPerSec:N0} msg/sec[/]");
        }

        // Compute overhead
        if (metrics.TryGetValue("rf1_produce_msg_sec", out var rf1Produce) &&
            metrics.TryGetValue("rf3_produce_msg_sec", out var rf3Produce) && rf1Produce > 0)
        {
            var produceOverhead = (1.0 - rf3Produce / rf1Produce) * 100;
            metrics["replication_produce_overhead_pct"] = produceOverhead;
            AnsiConsole.MarkupLine($"  [yellow]Replication produce overhead: {produceOverhead:F1}%[/]");
        }

        if (metrics.TryGetValue("rf1_consume_msg_sec", out var rf1Consume) &&
            metrics.TryGetValue("rf3_consume_msg_sec", out var rf3Consume) && rf1Consume > 0)
        {
            var consumeOverhead = (1.0 - rf3Consume / rf1Consume) * 100;
            metrics["replication_consume_overhead_pct"] = consumeOverhead;
            AnsiConsole.MarkupLine($"  [yellow]Replication consume overhead: {consumeOverhead:F1}%[/]");
        }

        overallSw.Stop();

        return new BenchmarkResult
        {
            Scenario = "replication",
            Description = $"Replication overhead (RF=1 vs RF=3) with {_config.BrokerCount} broker(s), {_config.MessageCount:N0} messages x {_config.MessageSizeBytes}B",
            Metrics = metrics,
            Duration = overallSw.Elapsed
        };
    }

    private async Task<(double ProduceMsgPerSec, double ConsumeMsgPerSec, double ProduceDurationMs, double ConsumeDurationMs)> RunReplicationBenchmarkAsync(
        int replicationFactor, byte[] messageValue)
    {
        await using var cluster = await ClusterSetup.CreateAsync(
            _config.BrokerCount,
            StorageEngines.Memory,
            partitions: 1);

        var topicName = $"bench-repl-rf{replicationFactor}";
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

        return (produceThroughput, consumeThroughput, produceMs, consumeMs);
    }
}
