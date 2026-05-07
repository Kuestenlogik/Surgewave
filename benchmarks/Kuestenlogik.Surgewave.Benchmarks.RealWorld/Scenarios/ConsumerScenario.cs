using System.Diagnostics;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Client.Native.Operations.Performance;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Benchmarks.RealWorld.Scenarios;

/// <summary>
/// Measures consumer performance: pre-loads a topic with messages, then consumes with
/// varying numbers of parallel consumers to measure consumer throughput scaling.
/// </summary>
public sealed class ConsumerScenario
{
    private readonly BenchmarkConfig _config;

    public ConsumerScenario(BenchmarkConfig config)
    {
        _config = config;
    }

    /// <summary>
    /// Pre-loads a topic with messages, then consumes with 1, 3, and 5 concurrent consumers
    /// (each on separate offset ranges) measuring total consume throughput and scaling factor.
    /// </summary>
    public async Task<BenchmarkResult> RunAsync()
    {
        var consumerCounts = new[] { 1, 3, 5 };
        AnsiConsole.MarkupLine("[bold cyan]Consumer Scenario[/]");
        AnsiConsole.MarkupLine($"  Brokers: {_config.BrokerCount}, Messages: {_config.MessageCount:N0}, Size: {_config.MessageSizeBytes}B");

        var overallSw = Stopwatch.StartNew();
        var metrics = new Dictionary<string, double>();

        var messageValue = new byte[_config.MessageSizeBytes];
        Random.Shared.NextBytes(messageValue);

        var baselineThroughput = 0.0;

        foreach (var consumerCount in consumerCounts)
        {
            AnsiConsole.MarkupLine($"  [dim]Testing {consumerCount} consumer(s)...[/]");

            var result = await RunConsumerBenchmarkAsync(consumerCount, messageValue);

            if (consumerCount == 1)
                baselineThroughput = result.Throughput;

            var scalingFactor = baselineThroughput > 0 ? result.Throughput / baselineThroughput : 1.0;

            metrics[$"consumers_{consumerCount}_throughput_msg_sec"] = result.Throughput;
            metrics[$"consumers_{consumerCount}_scaling_factor"] = scalingFactor;
            metrics[$"consumers_{consumerCount}_messages_consumed"] = result.Consumed;
            metrics[$"consumers_{consumerCount}_duration_ms"] = result.DurationMs;

            AnsiConsole.MarkupLine(
                $"  [green]{consumerCount} consumer(s): {result.Throughput:N0} msg/sec (scaling: {scalingFactor:F2}x, consumed: {result.Consumed:N0})[/]");
        }

        overallSw.Stop();

        return new BenchmarkResult
        {
            Scenario = "consumer",
            Description = $"Consumer scaling ({string.Join(", ", consumerCounts)} consumers) with {_config.BrokerCount} broker(s), {_config.MessageCount:N0} messages x {_config.MessageSizeBytes}B",
            Metrics = metrics,
            Duration = overallSw.Elapsed
        };
    }

    private async Task<(double Throughput, int Consumed, double DurationMs)> RunConsumerBenchmarkAsync(
        int consumerCount, byte[] messageValue)
    {
        var partitions = Math.Max(consumerCount, 1);

        await using var cluster = await ClusterSetup.CreateAsync(
            _config.BrokerCount,
            StorageEngines.Memory,
            partitions);

        var topicName = $"bench-consumer-{consumerCount}";
        await cluster.CreateTopicAsync(topicName, partitions);

        // Pre-load: produce to partition 0
        AnsiConsole.MarkupLine($"    [dim]Pre-loading {_config.MessageCount:N0} messages...[/]");
        await PreloadMessagesAsync(cluster, topicName, messageValue);

        // Consume: each consumer reads from partition 0 at different offset ranges
        var messagesPerConsumer = _config.MessageCount / consumerCount;
        var consumeTasks = new Task<int>[consumerCount];

        var consumeSw = Stopwatch.StartNew();

        for (int c = 0; c < consumerCount; c++)
        {
            var startOffset = (long)c * messagesPerConsumer;
            var endCount = c == consumerCount - 1 ? _config.MessageCount - c * messagesPerConsumer : messagesPerConsumer;
            var timeoutMs = _config.DurationSeconds * 1000;

            consumeTasks[c] = ConsumePartitionAsync(cluster, topicName, 0, startOffset, endCount, timeoutMs);
        }

        var consumed = (await Task.WhenAll(consumeTasks)).Sum();
        consumeSw.Stop();

        var consumeMs = Math.Max(1, consumeSw.ElapsedMilliseconds);
        var throughput = consumed * 1000.0 / consumeMs;

        return (throughput, consumed, consumeMs);
    }

    private async Task PreloadMessagesAsync(ClusterSetup cluster, string topicName, byte[] messageValue)
    {
        await using var client = await cluster.GetClientAsync(0);
        await using var producer = new SurgewaveBatchingProducer(
            client, topicName, partition: 0, maxBatchSize: _config.BatchSize, lingerTime: TimeSpan.FromMilliseconds(5));

        for (int i = 0; i < _config.MessageCount; i++)
        {
            await producer.ProduceAsync(null, messageValue);
        }

        await producer.FlushAsync();
    }

    private static async Task<int> ConsumePartitionAsync(
        ClusterSetup cluster,
        string topic,
        int partition,
        long startOffset,
        int targetCount,
        int timeoutMs)
    {
        await using var client = await cluster.GetClientAsync(0);

        var consumed = 0;
        var offset = startOffset;
        var sw = Stopwatch.StartNew();

        while (consumed < targetCount && sw.ElapsedMilliseconds < timeoutMs)
        {
            var result = await client.Messaging.ReceiveAsync(topic, partition, offset, 1024 * 1024);
            if (result.Messages.Count == 0)
            {
                await Task.Delay(5);
                continue;
            }

            consumed += result.Messages.Count;
            offset = result.Messages[^1].Offset + 1;
        }

        return consumed;
    }
}
