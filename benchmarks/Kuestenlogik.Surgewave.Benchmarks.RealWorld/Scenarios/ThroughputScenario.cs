using System.Diagnostics;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Client.Native.Operations.Performance;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Benchmarks.RealWorld.Scenarios;

/// <summary>
/// Measures maximum throughput under realistic conditions with an embedded multi-broker cluster.
/// Tests produce and consume performance at multiple message sizes and batch configurations.
/// </summary>
public sealed class ThroughputScenario
{
    private readonly BenchmarkConfig _config;

    public ThroughputScenario(BenchmarkConfig config)
    {
        _config = config;
    }

    /// <summary>
    /// Runs the throughput benchmark: produces N messages, then consumes them all,
    /// measuring messages/sec and MB/sec for both operations.
    /// </summary>
    public async Task<BenchmarkResult> RunAsync()
    {
        AnsiConsole.MarkupLine("[bold cyan]Throughput Scenario[/]");
        AnsiConsole.MarkupLine($"  Brokers: {_config.BrokerCount}, Messages: {_config.MessageCount:N0}, Size: {_config.MessageSizeBytes}B, Batch: {_config.BatchSize}");

        var overallSw = Stopwatch.StartNew();
        var metrics = new Dictionary<string, double>();

        await using var cluster = await ClusterSetup.CreateAsync(
            _config.BrokerCount,
            StorageEngines.Memory,
            _config.Partitions);

        var topicName = "bench-throughput";
        await cluster.CreateTopicAsync(topicName, _config.Partitions);

        var messageValue = new byte[_config.MessageSizeBytes];
        Random.Shared.NextBytes(messageValue);

        // --- Producer benchmark ---
        AnsiConsole.MarkupLine("  [dim]Producing...[/]");
        await using var client = await cluster.GetClientAsync(0);

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
        var produceMsgPerSec = _config.MessageCount * 1000.0 / produceMs;
        var produceMBPerSec = (long)_config.MessageCount * _config.MessageSizeBytes / 1024.0 / 1024.0 * 1000.0 / produceMs;

        metrics["produce_throughput_msg_sec"] = produceMsgPerSec;
        metrics["produce_throughput_mb_sec"] = produceMBPerSec;
        metrics["produce_duration_ms"] = produceMs;

        AnsiConsole.MarkupLine($"  [green]Produce: {produceMsgPerSec:N0} msg/sec, {produceMBPerSec:N1} MB/sec[/]");

        // --- Consumer benchmark ---
        AnsiConsole.MarkupLine("  [dim]Consuming...[/]");

        var consumeSw = Stopwatch.StartNew();
        var consumed = 0;
        long offset = 0;
        const int maxBytesPerFetch = 1024 * 1024;

        while (consumed < _config.MessageCount)
        {
            var result = await client.Messaging.ReceiveAsync(topicName, 0, offset, maxBytesPerFetch);
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
        var consumeMsgPerSec = consumed * 1000.0 / consumeMs;
        var consumeMBPerSec = (long)consumed * _config.MessageSizeBytes / 1024.0 / 1024.0 * 1000.0 / consumeMs;

        metrics["consume_throughput_msg_sec"] = consumeMsgPerSec;
        metrics["consume_throughput_mb_sec"] = consumeMBPerSec;
        metrics["consume_duration_ms"] = consumeMs;
        metrics["messages_consumed"] = consumed;

        AnsiConsole.MarkupLine($"  [green]Consume: {consumeMsgPerSec:N0} msg/sec, {consumeMBPerSec:N1} MB/sec[/]");

        overallSw.Stop();

        return new BenchmarkResult
        {
            Scenario = "throughput",
            Description = $"Max throughput with {_config.BrokerCount} broker(s), {_config.MessageCount:N0} messages x {_config.MessageSizeBytes}B, batch={_config.BatchSize}",
            Metrics = metrics,
            Duration = overallSw.Elapsed
        };
    }
}
