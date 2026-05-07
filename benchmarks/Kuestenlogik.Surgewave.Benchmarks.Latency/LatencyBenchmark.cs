using System.Diagnostics;
using Confluent.Kafka;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Client.Native.Operations.Performance;
using Kuestenlogik.Surgewave.Runtime;

namespace Kuestenlogik.Surgewave.Benchmarks.Latency;

/// <summary>
/// Latency benchmark measuring P50, P99, P99.9, P99.99 percentiles
/// for produce and consume operations.
/// Compares Surgewave Native protocol vs Confluent Kafka client.
///
/// Run with: dotnet run -- latency [msgCount] [msgSize] [storage]
///
/// Categories: Latency, P50, P90, P99, P99.9, P99.99, EndToEnd, Native, Kafka
/// </summary>
public static class LatencyBenchmark
{
    public static async Task RunAsync(int messageCount, int messageSize, string storageMode)
    {
        Console.WriteLine("╔══════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║     LATENCY BENCHMARK (P50/P99/P99.9/P99.99)                     ║");
        Console.WriteLine("╚══════════════════════════════════════════════════════════════════╝");
        Console.WriteLine($"Messages:     {messageCount:N0}");
        Console.WriteLine($"Message size: {messageSize} bytes");
        Console.WriteLine($"Storage:      {storageMode}");
        Console.WriteLine();

        var storage = storageMode.ToLowerInvariant() switch
        {
            "memory" => StorageEngines.Memory,
            "file" => StorageEngines.File,
            "arrow" => "arrow",
            _ => StorageEngines.Memory
        };

        await using var surgewave = await SurgewaveRuntime.CreateBuilder()
            .WithPort(0)
            .WithAutoCreateTopics(true)
            .WithPartitions(1)
            .WithStorageEngine(storage)
            .Build()
            .StartAsync();

        var host = "localhost";
        var port = surgewave.Port;

        // ═══════════════════════════════════════════════════════════════
        // Surgewave NATIVE PROTOCOL
        // ═══════════════════════════════════════════════════════════════
        Console.WriteLine();
        Console.WriteLine("╔══════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║     Surgewave NATIVE PROTOCOL                                        ║");
        Console.WriteLine("╚══════════════════════════════════════════════════════════════════╝");

        var nativeTopic = "latency-native";

        Console.WriteLine("Warming up (Native)...");
        await WarmupNativeAsync(host, port, nativeTopic, Math.Min(1000, messageCount / 10), messageSize);

        Console.WriteLine();
        Console.WriteLine("─── Producer Latency (Native) ───");
        var nativeProduceLatencies = await MeasureNativeProduceLatencyAsync(host, port, nativeTopic, messageCount, messageSize);
        PrintLatencyStats("Native Produce", nativeProduceLatencies);

        Console.WriteLine();
        Console.WriteLine("─── Consumer Latency (Native) ───");
        var nativeConsumeLatencies = await MeasureNativeConsumeLatencyAsync(host, port, nativeTopic, messageCount);
        PrintLatencyStats("Native Consume", nativeConsumeLatencies);

        Console.WriteLine();
        Console.WriteLine("─── End-to-End Latency (Native) ───");
        var nativeE2eLatencies = await MeasureNativeEndToEndLatencyAsync(host, port, nativeTopic + "-e2e", Math.Min(messageCount, 5000), messageSize);
        PrintLatencyStats("Native E2E", nativeE2eLatencies);

        // ═══════════════════════════════════════════════════════════════
        // CONFLUENT KAFKA CLIENT
        // ═══════════════════════════════════════════════════════════════
        Console.WriteLine();
        Console.WriteLine("╔══════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║     CONFLUENT KAFKA CLIENT (Wire Protocol)                       ║");
        Console.WriteLine("╚══════════════════════════════════════════════════════════════════╝");

        var kafkaTopic = "latency-kafka";
        var endpoint = $"{host}:{port}";

        Console.WriteLine("Warming up (Kafka)...");
        await WarmupKafkaAsync(endpoint, kafkaTopic, Math.Min(1000, messageCount / 10), messageSize);

        Console.WriteLine();
        Console.WriteLine("─── Producer Latency (Kafka) ───");
        var kafkaProduceLatencies = await MeasureKafkaProduceLatencyAsync(endpoint, kafkaTopic, messageCount, messageSize);
        PrintLatencyStats("Kafka Produce", kafkaProduceLatencies);

        Console.WriteLine();
        Console.WriteLine("─── Consumer Latency (Kafka) ───");
        var kafkaConsumeLatencies = await MeasureKafkaConsumeLatencyAsync(endpoint, kafkaTopic, messageCount);
        PrintLatencyStats("Kafka Consume", kafkaConsumeLatencies);

        Console.WriteLine();
        Console.WriteLine("─── End-to-End Latency (Kafka) ───");
        var kafkaE2eLatencies = await MeasureKafkaEndToEndLatencyAsync(endpoint, kafkaTopic + "-e2e", Math.Min(messageCount, 5000), messageSize);
        PrintLatencyStats("Kafka E2E", kafkaE2eLatencies);

        // ═══════════════════════════════════════════════════════════════
        // COMPARISON SUMMARY
        // ═══════════════════════════════════════════════════════════════
        Console.WriteLine();
        Console.WriteLine("╔══════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║     COMPARISON SUMMARY                                           ║");
        Console.WriteLine("╚══════════════════════════════════════════════════════════════════╝");
        PrintComparisonTable(
            nativeProduceLatencies, kafkaProduceLatencies,
            nativeConsumeLatencies, kafkaConsumeLatencies,
            nativeE2eLatencies, kafkaE2eLatencies);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Surgewave NATIVE PROTOCOL METHODS
    // ═══════════════════════════════════════════════════════════════════════

    private static async Task WarmupNativeAsync(string host, int port, string topic, int count, int messageSize)
    {
        await using var client = new SurgewaveNativeClient(host, port);
        await client.ConnectAsync();
        await client.Topics.CreateAsync(topic, 1);

        await using var producer = new SurgewaveBatchingProducer(client, topic, 0, maxBatchSize: 100);

        var payload = new byte[messageSize];
        Random.Shared.NextBytes(payload);

        for (int i = 0; i < count; i++)
        {
            await producer.ProduceAsync(null, payload);
        }
        await producer.FlushAsync();
    }

    private static async Task<long[]> MeasureNativeProduceLatencyAsync(string host, int port, string topic, int count, int messageSize)
    {
        await using var client = new SurgewaveNativeClient(host, port);
        await client.ConnectAsync();

        await using var producer = new SurgewaveBatchingProducer(client, topic, 0, maxBatchSize: 1, lingerTime: TimeSpan.Zero);

        var payload = new byte[messageSize];
        Random.Shared.NextBytes(payload);

        var latencies = new long[count];
        var sw = new Stopwatch();

        for (int i = 0; i < count; i++)
        {
            sw.Restart();
            await producer.ProduceAsync(null, payload);
            await producer.FlushAsync();
            sw.Stop();
            latencies[i] = sw.ElapsedTicks;

            if ((i + 1) % 10000 == 0)
            {
                Console.WriteLine($"  Produced {i + 1:N0} messages...");
            }
        }

        return latencies;
    }

    private static async Task<long[]> MeasureNativeConsumeLatencyAsync(string host, int port, string topic, int count)
    {
        await using var client = new SurgewaveNativeClient(host, port);
        await client.ConnectAsync();

        // Use prefetching consumer for fair comparison with Kafka (which also prefetches)
        await using var consumer = new SurgewavePrefetchingConsumer(
            client, topic, partition: 0, startOffset: 0,
            prefetchCount: count + 1000, // Buffer all messages + headroom
            maxBytesPerFetch: 1024 * 1024);

        // Let prefetcher warm up and buffer some messages
        await Task.Delay(100);

        var latencies = new long[count];
        var sw = new Stopwatch();

        for (int i = 0; i < count; i++)
        {
            sw.Restart();
            var msg = consumer.Consume(); // Hot path: just array index increment
            sw.Stop();

            if (msg != null)
            {
                latencies[i] = sw.ElapsedTicks;
            }
            else
            {
                // Wait for more messages if buffer empty
                msg = await consumer.ConsumeAsync();
                latencies[i] = sw.ElapsedTicks;
            }

            if ((i + 1) % 10000 == 0)
            {
                Console.WriteLine($"  Consumed {i + 1:N0} messages...");
            }
        }

        return latencies;
    }

    private static async Task<long[]> MeasureNativeEndToEndLatencyAsync(string host, int port, string topic, int count, int messageSize)
    {
        await using var client = new SurgewaveNativeClient(host, port);
        await client.ConnectAsync();
        await client.Topics.CreateAsync(topic, 1);

        await using var producer = new SurgewaveBatchingProducer(client, topic, 0, maxBatchSize: 1, lingerTime: TimeSpan.Zero);

        var payload = new byte[messageSize];
        var latencies = new long[count];
        var sw = new Stopwatch();
        long offset = 0;

        for (int i = 0; i < count; i++)
        {
            BitConverter.TryWriteBytes(payload.AsSpan(), Stopwatch.GetTimestamp());

            sw.Restart();
            await producer.ProduceAsync(null, payload);
            await producer.FlushAsync();

            // Fetch immediately
            while (true)
            {
                var result = await client.Messaging.ReceiveAsync(topic, 0, offset, 64 * 1024);
                if (result.Messages.Count > 0)
                {
                    offset = result.Messages[^1].Offset + 1;
                    break;
                }
                await Task.Delay(1);
            }
            sw.Stop();

            latencies[i] = sw.ElapsedTicks;

            if ((i + 1) % 1000 == 0)
            {
                Console.WriteLine($"  Completed {i + 1:N0} round-trips...");
            }
        }

        return latencies;
    }

    // ═══════════════════════════════════════════════════════════════════════
    // CONFLUENT KAFKA CLIENT METHODS
    // ═══════════════════════════════════════════════════════════════════════

    private static async Task WarmupKafkaAsync(string endpoint, string topic, int count, int messageSize)
    {
        var producerConfig = new ProducerConfig { BootstrapServers = endpoint };
        using var producer = new ProducerBuilder<Null, byte[]>(producerConfig).Build();

        var payload = new byte[messageSize];
        Random.Shared.NextBytes(payload);

        for (int i = 0; i < count; i++)
        {
            await producer.ProduceAsync(topic, new Message<Null, byte[]> { Value = payload });
        }
        producer.Flush(TimeSpan.FromSeconds(10));
    }

    private static async Task<long[]> MeasureKafkaProduceLatencyAsync(string endpoint, string topic, int count, int messageSize)
    {
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = endpoint,
            Acks = Acks.All
        };
        using var producer = new ProducerBuilder<Null, byte[]>(producerConfig).Build();

        var payload = new byte[messageSize];
        Random.Shared.NextBytes(payload);

        var latencies = new long[count];
        var sw = new Stopwatch();

        for (int i = 0; i < count; i++)
        {
            sw.Restart();
            await producer.ProduceAsync(topic, new Message<Null, byte[]> { Value = payload });
            sw.Stop();
            latencies[i] = sw.ElapsedTicks;

            if ((i + 1) % 10000 == 0)
            {
                Console.WriteLine($"  Produced {i + 1:N0} messages...");
            }
        }

        producer.Flush(TimeSpan.FromSeconds(10));
        return latencies;
    }

    private static Task<long[]> MeasureKafkaConsumeLatencyAsync(string endpoint, string topic, int count)
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = endpoint,
            GroupId = $"latency-kafka-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        using var consumer = new ConsumerBuilder<Ignore, byte[]>(consumerConfig).Build();
        consumer.Subscribe(topic);

        var latencies = new List<long>(count);
        var sw = new Stopwatch();
        var consumed = 0;

        while (consumed < count)
        {
            sw.Restart();
            var result = consumer.Consume(TimeSpan.FromSeconds(5));
            sw.Stop();

            if (result != null)
            {
                latencies.Add(sw.ElapsedTicks);
                consumed++;

                if (consumed % 10000 == 0)
                {
                    Console.WriteLine($"  Consumed {consumed:N0} messages...");
                }
            }
        }

        return Task.FromResult(latencies.ToArray());
    }

    private static async Task<long[]> MeasureKafkaEndToEndLatencyAsync(string endpoint, string topic, int count, int messageSize)
    {
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = endpoint,
            Acks = Acks.All
        };
        using var producer = new ProducerBuilder<Null, byte[]>(producerConfig).Build();

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = endpoint,
            GroupId = $"e2e-kafka-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };
        using var consumer = new ConsumerBuilder<Ignore, byte[]>(consumerConfig).Build();
        consumer.Subscribe(topic);

        var payload = new byte[messageSize];
        var latencies = new long[count];
        var sw = new Stopwatch();

        for (int i = 0; i < count; i++)
        {
            BitConverter.TryWriteBytes(payload.AsSpan(), Stopwatch.GetTimestamp());

            sw.Restart();
            await producer.ProduceAsync(topic, new Message<Null, byte[]> { Value = payload });
            producer.Flush(TimeSpan.FromSeconds(1));
            _ = consumer.Consume(TimeSpan.FromSeconds(5));
            sw.Stop();

            latencies[i] = sw.ElapsedTicks;

            if ((i + 1) % 1000 == 0)
            {
                Console.WriteLine($"  Completed {i + 1:N0} round-trips...");
            }
        }

        return latencies;
    }

    // ═══════════════════════════════════════════════════════════════════════
    // OUTPUT FORMATTING
    // ═══════════════════════════════════════════════════════════════════════

    private static void PrintLatencyStats(string label, long[] latencies)
    {
        if (latencies.Length == 0)
        {
            Console.WriteLine($"  No data for {label}");
            return;
        }

        Array.Sort(latencies);

        var ticksPerMicrosecond = Stopwatch.Frequency / 1_000_000.0;

        var min = latencies[0];
        var max = latencies[^1];
        var p50 = GetPercentile(latencies, 50);
        var p90 = GetPercentile(latencies, 90);
        var p99 = GetPercentile(latencies, 99);
        var p999 = GetPercentile(latencies, 99.9);
        var p9999 = GetPercentile(latencies, 99.99);
        var avg = latencies.Average();

        Console.WriteLine($"  Samples:  {latencies.Length:N0}");
        Console.WriteLine();
        Console.WriteLine("  Percentile      Latency");
        Console.WriteLine("  ─────────────────────────────");
        Console.WriteLine($"  Min           {FormatLatency(min, ticksPerMicrosecond)}");
        Console.WriteLine($"  P50 (median)  {FormatLatency(p50, ticksPerMicrosecond)}");
        Console.WriteLine($"  P90           {FormatLatency(p90, ticksPerMicrosecond)}");
        Console.WriteLine($"  P99           {FormatLatency(p99, ticksPerMicrosecond)}");
        Console.WriteLine($"  P99.9         {FormatLatency(p999, ticksPerMicrosecond)}");
        Console.WriteLine($"  P99.99        {FormatLatency(p9999, ticksPerMicrosecond)}");
        Console.WriteLine($"  Max           {FormatLatency(max, ticksPerMicrosecond)}");
        Console.WriteLine($"  Avg           {FormatLatency((long)avg, ticksPerMicrosecond)}");
    }

    private static void PrintComparisonTable(
        long[] nativeProduce, long[] kafkaProduce,
        long[] nativeConsume, long[] kafkaConsume,
        long[] nativeE2e, long[] kafkaE2e)
    {
        var tpm = Stopwatch.Frequency / 1_000_000.0;

        Array.Sort(nativeProduce);
        Array.Sort(kafkaProduce);
        Array.Sort(nativeConsume);
        Array.Sort(kafkaConsume);
        Array.Sort(nativeE2e);
        Array.Sort(kafkaE2e);

        Console.WriteLine();
        Console.WriteLine("┌────────────────┬────────────────────────────────────────────────────────────┐");
        Console.WriteLine("│                │                    LATENCY (microseconds)                  │");
        Console.WriteLine("│    Operation   ├──────────┬──────────┬──────────┬──────────┬───────────────┤");
        Console.WriteLine("│                │   P50    │   P99    │  P99.9   │  P99.99  │ Native vs Kafka│");
        Console.WriteLine("├────────────────┼──────────┼──────────┼──────────┼──────────┼───────────────┤");

        PrintComparisonRow("Produce Native", nativeProduce, null, tpm);
        PrintComparisonRow("Produce Kafka", kafkaProduce, nativeProduce, tpm);
        Console.WriteLine("├────────────────┼──────────┼──────────┼──────────┼──────────┼───────────────┤");
        PrintComparisonRow("Consume Native", nativeConsume, null, tpm);
        PrintComparisonRow("Consume Kafka", kafkaConsume, nativeConsume, tpm);
        Console.WriteLine("├────────────────┼──────────┼──────────┼──────────┼──────────┼───────────────┤");
        PrintComparisonRow("E2E Native", nativeE2e, null, tpm);
        PrintComparisonRow("E2E Kafka", kafkaE2e, nativeE2e, tpm);

        Console.WriteLine("└────────────────┴──────────┴──────────┴──────────┴──────────┴───────────────┘");

        // Summary
        var nativeP99Produce = GetPercentile(nativeProduce, 99) / tpm;
        var kafkaP99Produce = GetPercentile(kafkaProduce, 99) / tpm;
        var nativeP99Consume = GetPercentile(nativeConsume, 99) / tpm;
        var kafkaP99Consume = GetPercentile(kafkaConsume, 99) / tpm;

        Console.WriteLine();
        Console.WriteLine("Native Protocol Advantage (P99):");
        Console.WriteLine($"  Producer: {kafkaP99Produce / nativeP99Produce:F1}x faster");
        Console.WriteLine($"  Consumer: {kafkaP99Consume / nativeP99Consume:F1}x faster");
    }

    private static void PrintComparisonRow(string label, long[] data, long[]? baseline, double tpm)
    {
        var p50 = GetPercentile(data, 50) / tpm;
        var p99 = GetPercentile(data, 99) / tpm;
        var p999 = GetPercentile(data, 99.9) / tpm;
        var p9999 = GetPercentile(data, 99.99) / tpm;

        var comparison = "";
        if (baseline != null)
        {
            var baseP99 = GetPercentile(baseline, 99) / tpm;
            var ratio = p99 / baseP99;
            comparison = ratio > 1 ? $"{ratio:F1}x slower" : $"{1 / ratio:F1}x faster";
        }

        Console.WriteLine($"│ {label,-14} │ {p50,8:F0} │ {p99,8:F0} │ {p999,8:F0} │ {p9999,8:F0} │ {comparison,-13} │");
    }

    private static string FormatLatency(long ticks, double ticksPerMicrosecond)
    {
        var microseconds = ticks / ticksPerMicrosecond;

        if (microseconds < 1000)
            return $"{microseconds,8:F2} µs";
        else if (microseconds < 1_000_000)
            return $"{microseconds / 1000,8:F2} ms";
        else
            return $"{microseconds / 1_000_000,8:F2} s";
    }

    private static long GetPercentile(long[] sortedData, double percentile)
    {
        if (sortedData.Length == 0) return 0;
        if (sortedData.Length == 1) return sortedData[0];

        var index = (percentile / 100.0) * (sortedData.Length - 1);
        var lower = (int)Math.Floor(index);
        var upper = (int)Math.Ceiling(index);

        if (lower == upper || upper >= sortedData.Length)
            return sortedData[lower];

        var fraction = index - lower;
        return (long)(sortedData[lower] + fraction * (sortedData[upper] - sortedData[lower]));
    }
}

/// <summary>
/// HDR Histogram-style latency recorder for continuous monitoring.
/// </summary>
public sealed class LatencyRecorder
{
    private readonly long[] _samples;
    private int _count;
    private readonly object _lock = new();

    public LatencyRecorder(int maxSamples = 100_000)
    {
        _samples = new long[maxSamples];
    }

    public void Record(long ticks)
    {
        lock (_lock)
        {
            if (_count < _samples.Length)
            {
                _samples[_count++] = ticks;
            }
        }
    }

    public LatencyStats GetStats()
    {
        long[] snapshot;
        lock (_lock)
        {
            snapshot = new long[_count];
            Array.Copy(_samples, snapshot, _count);
        }

        if (snapshot.Length == 0)
            return new LatencyStats();

        Array.Sort(snapshot);

        return new LatencyStats
        {
            Count = snapshot.Length,
            Min = snapshot[0],
            Max = snapshot[^1],
            P50 = GetPercentile(snapshot, 50),
            P90 = GetPercentile(snapshot, 90),
            P99 = GetPercentile(snapshot, 99),
            P999 = GetPercentile(snapshot, 99.9),
            P9999 = GetPercentile(snapshot, 99.99),
            Avg = (long)snapshot.Average()
        };
    }

    public void Reset()
    {
        lock (_lock)
        {
            _count = 0;
        }
    }

    private static long GetPercentile(long[] sortedData, double percentile)
    {
        var index = (percentile / 100.0) * (sortedData.Length - 1);
        var lower = (int)Math.Floor(index);
        var upper = Math.Min((int)Math.Ceiling(index), sortedData.Length - 1);

        if (lower == upper)
            return sortedData[lower];

        var fraction = index - lower;
        return (long)(sortedData[lower] + fraction * (sortedData[upper] - sortedData[lower]));
    }
}

/// <summary>
/// Latency statistics in ticks (use Stopwatch.Frequency to convert).
/// </summary>
public record struct LatencyStats
{
    public int Count { get; set; }
    public long Min { get; set; }
    public long Max { get; set; }
    public long P50 { get; set; }
    public long P90 { get; set; }
    public long P99 { get; set; }
    public long P999 { get; set; }
    public long P9999 { get; set; }
    public long Avg { get; set; }

    public readonly double MinMicroseconds => Min * 1_000_000.0 / Stopwatch.Frequency;
    public readonly double MaxMicroseconds => Max * 1_000_000.0 / Stopwatch.Frequency;
    public readonly double P50Microseconds => P50 * 1_000_000.0 / Stopwatch.Frequency;
    public readonly double P99Microseconds => P99 * 1_000_000.0 / Stopwatch.Frequency;
    public readonly double P999Microseconds => P999 * 1_000_000.0 / Stopwatch.Frequency;
    public readonly double P9999Microseconds => P9999 * 1_000_000.0 / Stopwatch.Frequency;

    public override readonly string ToString()
    {
        return $"P50={P50Microseconds:F1}µs P99={P99Microseconds:F1}µs P99.9={P999Microseconds:F1}µs P99.99={P9999Microseconds:F1}µs";
    }
}
