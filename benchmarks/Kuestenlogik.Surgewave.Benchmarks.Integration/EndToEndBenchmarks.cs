using System.Diagnostics;
using System.Text.Json;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Confluent.Kafka;

namespace Kuestenlogik.Surgewave.Benchmarks.Integration;

internal static class EndToEndJsonOptions
{
    public static readonly JsonSerializerOptions CaseInsensitive = new() { PropertyNameCaseInsensitive = true };
    public static readonly JsonSerializerOptions Indented = new() { WriteIndented = true };
}

/// <summary>
/// End-to-end throughput benchmarks against a running Surgewave broker.
/// Measures actual produce and consume performance including network and disk I/O.
///
/// IMPORTANT: Run Surgewave broker before executing these benchmarks:
///   dotnet run --project src/Kuestenlogik.Surgewave.Broker
/// </summary>
[SimpleJob(RuntimeMoniker.HostProcess, launchCount: 1, warmupCount: 1, iterationCount: 3)]
[MemoryDiagnoser]
[BenchmarkCategory("Integration", "Throughput", "Kafka")]
public class EndToEndBenchmarks
{
    private const string BootstrapServers = "localhost:9092";
    private const string TopicName = "benchmark-topic";

    private IProducer<string, byte[]> _producer = null!;
    private byte[] _messageValue = null!;

    [Params(100_000, 1_000_000)]
    public int MessageCount { get; set; }

    [Params(100, 1024)]
    public int MessageSizeBytes { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _messageValue = new byte[MessageSizeBytes];
        Random.Shared.NextBytes(_messageValue);

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = BootstrapServers,
            Acks = Acks.Leader,  // acks=1 for speed
            LingerMs = 5,        // Batch messages
            BatchSize = 65536,   // 64KB batches
            CompressionType = CompressionType.None
        };

        _producer = new ProducerBuilder<string, byte[]>(producerConfig).Build();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _producer?.Flush(TimeSpan.FromSeconds(10));
        _producer?.Dispose();
    }

    /// <summary>
    /// Benchmark producing N messages to the broker (fire-and-forget)
    /// </summary>
    [Benchmark(Description = "Produce N messages (fire-and-forget)")]
    public async Task<int> Produce_FireAndForget()
    {
        var produced = 0;
        for (int i = 0; i < MessageCount; i++)
        {
            _producer.Produce(TopicName, new Message<string, byte[]>
            {
                Key = i.ToString(),
                Value = _messageValue
            });
            produced++;
        }

        // Flush remaining messages
        _producer.Flush(TimeSpan.FromSeconds(30));
        return produced;
    }

    /// <summary>
    /// Benchmark producing N messages with delivery confirmation
    /// </summary>
    [Benchmark(Description = "Produce N messages (with acks)")]
    public async Task<int> Produce_WithAcks()
    {
        var tasks = new List<Task<DeliveryResult<string, byte[]>>>(MessageCount);

        for (int i = 0; i < MessageCount; i++)
        {
            var task = _producer.ProduceAsync(TopicName, new Message<string, byte[]>
            {
                Key = i.ToString(),
                Value = _messageValue
            });
            tasks.Add(task);
        }

        await Task.WhenAll(tasks);
        return tasks.Count;
    }
}

/// <summary>
/// Simple console-based throughput test that can be run standalone.
/// More practical than BenchmarkDotNet for quick throughput measurements.
/// </summary>
public static class QuickThroughputTest
{
    public static async Task RunAsync(string[] args)
    {
        var messageCount = args.Length > 0 ? int.Parse(args[0]) : 1_000_000;
        var messageSizeBytes = args.Length > 1 ? int.Parse(args[1]) : 1024;
        var bootstrapServers = args.Length > 2 ? args[2] : "localhost:9092";
        var topicName = "throughput-test";

        Console.WriteLine($"Surgewave Throughput Test");
        Console.WriteLine($"=====================");
        Console.WriteLine($"Messages: {messageCount:N0}");
        Console.WriteLine($"Message size: {messageSizeBytes:N0} bytes");
        Console.WriteLine($"Total data: {(long)messageCount * messageSizeBytes / 1024 / 1024:N0} MB");
        Console.WriteLine($"Bootstrap: {bootstrapServers}");
        Console.WriteLine();

        var messageValue = new byte[messageSizeBytes];
        Random.Shared.NextBytes(messageValue);

        // Producer test
        Console.WriteLine("=== PRODUCER TEST ===");
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            Acks = Acks.Leader,
            LingerMs = 5,
            BatchSize = 65536,
            QueueBufferingMaxMessages = 2000000,  // Allow large buffer
            QueueBufferingMaxKbytes = 2097152,    // 2GB buffer
        };

        using var producer = new ProducerBuilder<string, byte[]>(producerConfig).Build();

        var sw = Stopwatch.StartNew();
        for (int i = 0; i < messageCount; i++)
        {
            producer.Produce(topicName, new Message<string, byte[]>
            {
                Key = i.ToString(),
                Value = messageValue
            });

            if (i > 0 && i % 100_000 == 0)
            {
                Console.WriteLine($"  Produced {i:N0} messages...");
            }
        }
        producer.Flush(TimeSpan.FromSeconds(60));
        sw.Stop();

        var produceMs = sw.ElapsedMilliseconds;
        var produceMsgPerSec = messageCount * 1000.0 / produceMs;
        var produceMBPerSec = (long)messageCount * messageSizeBytes / 1024.0 / 1024.0 * 1000.0 / produceMs;

        Console.WriteLine($"  Time: {produceMs:N0} ms");
        Console.WriteLine($"  Throughput: {produceMsgPerSec:N0} msg/sec");
        Console.WriteLine($"  Throughput: {produceMBPerSec:N1} MB/sec");
        Console.WriteLine();

        // Consumer test - use direct partition assignment for benchmarking (avoids group coordination overhead)
        Console.WriteLine("=== CONSUMER TEST ===");
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = $"throughput-test-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        };

        using var consumer = new ConsumerBuilder<string, byte[]>(consumerConfig).Build();

        // Use direct partition assignment instead of Subscribe for faster benchmarking
        var partitions = new List<TopicPartitionOffset>
        {
            new TopicPartitionOffset(topicName, 0, Offset.Beginning)
        };
        consumer.Assign(partitions);
        Console.WriteLine($"  Assigned partition {topicName}-0 from offset 0");

        sw.Restart();
        var consumed = 0;
        var noMessageCount = 0;
        const int maxNoMessageRetries = 5;

        while (consumed < messageCount && noMessageCount < maxNoMessageRetries)
        {
            var result = consumer.Consume(TimeSpan.FromSeconds(5));
            if (result == null)
            {
                noMessageCount++;
                Console.WriteLine($"  No message received (attempt {noMessageCount}/{maxNoMessageRetries})...");
                continue;
            }
            noMessageCount = 0; // Reset on successful consume
            consumed++;

            if (consumed > 0 && consumed % 100_000 == 0)
            {
                Console.WriteLine($"  Consumed {consumed:N0} messages...");
            }
        }
        sw.Stop();

        var consumeMs = sw.ElapsedMilliseconds;
        var consumeMsgPerSec = consumed * 1000.0 / consumeMs;
        var consumeMBPerSec = (long)consumed * messageSizeBytes / 1024.0 / 1024.0 * 1000.0 / consumeMs;

        Console.WriteLine($"  Messages: {consumed:N0}");
        Console.WriteLine($"  Time: {consumeMs:N0} ms");
        Console.WriteLine($"  Throughput: {consumeMsgPerSec:N0} msg/sec");
        Console.WriteLine($"  Throughput: {consumeMBPerSec:N1} MB/sec");
        Console.WriteLine();

        Console.WriteLine("=== SUMMARY ===");
        Console.WriteLine($"Producer: {produceMsgPerSec:N0} msg/sec ({produceMBPerSec:N1} MB/sec)");
        Console.WriteLine($"Consumer: {consumeMsgPerSec:N0} msg/sec ({consumeMBPerSec:N1} MB/sec)");
        Console.WriteLine();

        // Compare with baseline
        var baselinePath = FindBaselineFile();
        if (baselinePath != null)
        {
            await CompareWithBaselineAsync(
                baselinePath,
                produceMsgPerSec, produceMBPerSec,
                consumeMsgPerSec, consumeMBPerSec,
                messageCount, messageSizeBytes);
        }
    }

    private static string? FindBaselineFile()
    {
        // Search for baseline file in current directory and parent directories
        var searchPaths = new[]
        {
            "baselines/benchmark-baseline.json",
            "../baselines/benchmark-baseline.json",
            "../../benchmarks/baselines/benchmark-baseline.json",
            "../../../benchmarks/baselines/benchmark-baseline.json"
        };

        foreach (var path in searchPaths)
        {
            if (File.Exists(path))
                return Path.GetFullPath(path);
        }

        return null;
    }

    private static async Task CompareWithBaselineAsync(
        string baselinePath,
        double producerMsgPerSec, double producerMBPerSec,
        double consumerMsgPerSec, double consumerMBPerSec,
        int messageCount, int messageSizeBytes)
    {
        try
        {
            var json = await File.ReadAllTextAsync(baselinePath);
            var baseline = JsonSerializer.Deserialize<BenchmarkBaseline>(json, EndToEndJsonOptions.CaseInsensitive);

            if (baseline?.Baseline == null)
            {
                Console.WriteLine("No baseline data found.");
                return;
            }

            Console.WriteLine("=== COMPARISON WITH BASELINE ===");

            var producerDelta = (producerMsgPerSec - baseline.Baseline.Producer.MessagesPerSec) / baseline.Baseline.Producer.MessagesPerSec * 100;
            var consumerDelta = (consumerMsgPerSec - baseline.Baseline.Consumer.MessagesPerSec) / baseline.Baseline.Consumer.MessagesPerSec * 100;

            var producerSign = producerDelta >= 0 ? "+" : "";
            var consumerSign = consumerDelta >= 0 ? "+" : "";

            Console.WriteLine($"Producer: {baseline.Baseline.Producer.MessagesPerSec:N0} -> {producerMsgPerSec:N0} msg/sec ({producerSign}{producerDelta:N1}%)");
            Console.WriteLine($"Consumer: {baseline.Baseline.Consumer.MessagesPerSec:N0} -> {consumerMsgPerSec:N0} msg/sec ({consumerSign}{consumerDelta:N1}%)");
            Console.WriteLine();

            // Check if both producer and consumer are better
            var isBetter = producerMsgPerSec > baseline.Baseline.Producer.MessagesPerSec &&
                           consumerMsgPerSec > baseline.Baseline.Consumer.MessagesPerSec;

            if (isBetter)
            {
                Console.WriteLine("NEW BASELINE! Both producer and consumer improved.");
                Console.Write("Update baseline? (y/N): ");
                var response = Console.ReadLine()?.Trim().ToLowerInvariant();

                if (response == "y" || response == "yes")
                {
                    await UpdateBaselineAsync(baselinePath, baseline,
                        producerMsgPerSec, producerMBPerSec,
                        consumerMsgPerSec, consumerMBPerSec,
                        messageCount, messageSizeBytes);
                    Console.WriteLine("Baseline updated!");
                }
            }
            else if (producerDelta < -5 || consumerDelta < -5)
            {
                Console.WriteLine("WARNING: Performance regression detected (>5% slower)");
            }
            else
            {
                Console.WriteLine("Performance within baseline range.");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error reading baseline: {ex.Message}");
        }
    }

    private static async Task UpdateBaselineAsync(
        string baselinePath,
        BenchmarkBaseline baseline,
        double producerMsgPerSec, double producerMBPerSec,
        double consumerMsgPerSec, double consumerMBPerSec,
        int messageCount, int messageSizeBytes)
    {
        // Update baseline
        baseline.LastUpdated = DateTime.UtcNow.ToString("o");
        baseline.TestConfig = new TestConfig
        {
            MessageCount = messageCount,
            MessageSizeBytes = messageSizeBytes,
            TotalDataMB = messageCount * messageSizeBytes / 1024 / 1024
        };
        baseline.Baseline = new MetricsPair
        {
            Producer = new Metrics { MessagesPerSec = producerMsgPerSec, MBPerSec = producerMBPerSec },
            Consumer = new Metrics { MessagesPerSec = consumerMsgPerSec, MBPerSec = consumerMBPerSec }
        };

        // Add to history
        baseline.History ??= [];
        baseline.History.Add(new HistoryEntry
        {
            Date = DateTime.UtcNow.ToString("yyyy-MM-dd"),
            Commit = "current",
            Description = "Updated via benchmark comparison",
            Producer = new Metrics { MessagesPerSec = producerMsgPerSec, MBPerSec = producerMBPerSec },
            Consumer = new Metrics { MessagesPerSec = consumerMsgPerSec, MBPerSec = consumerMBPerSec }
        });

        var json = JsonSerializer.Serialize(baseline, EndToEndJsonOptions.Indented);
        await File.WriteAllTextAsync(baselinePath, json);
    }
}

// JSON model classes for baseline file
public sealed class BenchmarkBaseline
{
    public string? LastUpdated { get; set; }
    public TestConfig? TestConfig { get; set; }
    public MetricsPair? Baseline { get; set; }
    public List<HistoryEntry>? History { get; set; }
}

public class TestConfig
{
    public int MessageCount { get; set; }
    public int MessageSizeBytes { get; set; }
    public int TotalDataMB { get; set; }
}

public class MetricsPair
{
    public Metrics Producer { get; set; } = new();
    public Metrics Consumer { get; set; } = new();
}

public class Metrics
{
    public double MessagesPerSec { get; set; }
    public double MBPerSec { get; set; }
}

public class HistoryEntry
{
    public string? Date { get; set; }
    public string? Commit { get; set; }
    public string? Description { get; set; }
    public Metrics Producer { get; set; } = new();
    public Metrics Consumer { get; set; } = new();
}
