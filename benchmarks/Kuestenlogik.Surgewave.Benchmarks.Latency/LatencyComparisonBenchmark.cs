using System.Diagnostics;
using Confluent.Kafka;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Client.Native.Operations.Performance;
using Kuestenlogik.Surgewave.Runtime;

namespace Kuestenlogik.Surgewave.Benchmarks.Latency;

/// <summary>
/// Latency comparison benchmark: Surgewave (embedded) vs Real Apache Kafka broker.
/// Measures P50/P99/P99.9/P99.99 for produce, consume, and end-to-end operations.
///
/// Run with: dotnet run -- latency-compare [msgCount] [msgSize] [kafkaBootstrap]
/// Example:  dotnet run -- latency-compare 5000 100 localhost:29092
///
/// Categories: Latency, P50, P90, P99, P99.9, P99.99, EndToEnd, Comparison, Kafka, Native
/// </summary>
public static class LatencyComparisonBenchmark
{
    public static async Task RunAsync(int messageCount, int messageSize, string kafkaBootstrap)
    {
        Console.WriteLine("╔══════════════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║           LATENCY COMPARISON: Surgewave vs Apache Kafka                      ║");
        Console.WriteLine("╚══════════════════════════════════════════════════════════════════════════╝");
        Console.WriteLine();
        Console.WriteLine($"Configuration:");
        Console.WriteLine($"  Messages:       {messageCount:N0}");
        Console.WriteLine($"  Message size:   {messageSize} bytes");
        Console.WriteLine($"  Kafka broker:   {kafkaBootstrap}");
        Console.WriteLine();

        var payload = new byte[messageSize];
        Random.Shared.NextBytes(payload);

        // Results storage
        LatencyResult? kafkaResult = null;
        LatencyResult? surgewaveKafkaResult = null;
        LatencyResult? surgewaveNativeResult = null;

        // ═══════════════════════════════════════════════════════════════
        // TEST 1: PURE KAFKA (Apache Kafka broker)
        // ═══════════════════════════════════════════════════════════════
        Console.WriteLine("═══════════════════════════════════════════════════════════════════════════");
        Console.WriteLine("  [1/3] PURE KAFKA (Apache Kafka broker + Confluent.Kafka client)");
        Console.WriteLine("═══════════════════════════════════════════════════════════════════════════");

        try
        {
            kafkaResult = await MeasureKafkaLatencyAsync(kafkaBootstrap, messageCount, payload);
            PrintLatencyResult("Pure Kafka", kafkaResult);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"  SKIPPED: Kafka broker not available at {kafkaBootstrap}");
            Console.WriteLine($"  Error: {ex.Message}");
        }

        // ═══════════════════════════════════════════════════════════════
        // TEST 2/3: Surgewave BROKER
        // ═══════════════════════════════════════════════════════════════
        Console.WriteLine();
        Console.WriteLine("Starting embedded Surgewave broker...");
        await using var surgewave = await SurgewaveRuntime.CreateBuilder()
            .WithPort(0)
            .WithAutoCreateTopics(true)
            .WithPartitions(1)
            .Build()
            .StartAsync();
        Console.WriteLine($"Embedded Surgewave started on port {surgewave.Port}");

        // ═══════════════════════════════════════════════════════════════
        // TEST 2: Surgewave + KAFKA PROTOCOL
        // ═══════════════════════════════════════════════════════════════
        Console.WriteLine();
        Console.WriteLine("═══════════════════════════════════════════════════════════════════════════");
        Console.WriteLine("  [2/3] Surgewave + KAFKA PROTOCOL (Surgewave broker + Confluent.Kafka client)");
        Console.WriteLine("═══════════════════════════════════════════════════════════════════════════");

        surgewaveKafkaResult = await MeasureKafkaLatencyAsync(surgewave.BootstrapServers, messageCount, payload);
        PrintLatencyResult("Surgewave+Kafka", surgewaveKafkaResult);

        // ═══════════════════════════════════════════════════════════════
        // TEST 3: Surgewave NATIVE PROTOCOL
        // ═══════════════════════════════════════════════════════════════
        Console.WriteLine();
        Console.WriteLine("═══════════════════════════════════════════════════════════════════════════");
        Console.WriteLine("  [3/3] Surgewave NATIVE (Surgewave broker + SurgewaveNativeClient)");
        Console.WriteLine("═══════════════════════════════════════════════════════════════════════════");

        surgewaveNativeResult = await MeasureNativeLatencyAsync(surgewave.Host, surgewave.Port, messageCount, payload);
        PrintLatencyResult("Surgewave Native", surgewaveNativeResult);

        // ═══════════════════════════════════════════════════════════════
        // COMPARISON TABLE
        // ═══════════════════════════════════════════════════════════════
        Console.WriteLine();
        Console.WriteLine("╔══════════════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║                        LATENCY COMPARISON SUMMARY                        ║");
        Console.WriteLine("╚══════════════════════════════════════════════════════════════════════════╝");
        Console.WriteLine();

        PrintComparisonTable(kafkaResult, surgewaveKafkaResult, surgewaveNativeResult);
    }

    private static async Task<LatencyResult> MeasureKafkaLatencyAsync(string bootstrapServers, int count, byte[] payload)
    {
        var topicProduce = $"latency-produce-{Guid.NewGuid():N}";
        var topicE2e = $"latency-e2e-{Guid.NewGuid():N}";
        var tpm = Stopwatch.Frequency / 1_000_000.0;

        // Warmup
        Console.WriteLine("  Warming up...");
        var warmupConfig = new ProducerConfig { BootstrapServers = bootstrapServers };
        using (var warmup = new ProducerBuilder<Null, byte[]>(warmupConfig).Build())
        {
            for (int i = 0; i < Math.Min(100, count / 10); i++)
            {
                await warmup.ProduceAsync(topicProduce, new Message<Null, byte[]> { Value = payload });
            }
            warmup.Flush(TimeSpan.FromSeconds(10));
        }

        // Producer latency (sync, single message at a time)
        Console.WriteLine("  Measuring producer latency...");
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            Acks = Acks.All
        };

        var produceLatencies = new long[count];
        using (var producer = new ProducerBuilder<Null, byte[]>(producerConfig).Build())
        {
            var sw = new Stopwatch();
            for (int i = 0; i < count; i++)
            {
                sw.Restart();
                await producer.ProduceAsync(topicProduce, new Message<Null, byte[]> { Value = payload });
                sw.Stop();
                produceLatencies[i] = sw.ElapsedTicks;
            }
            producer.Flush(TimeSpan.FromSeconds(10));
        }

        // Consumer latency
        Console.WriteLine("  Measuring consumer latency...");
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = $"latency-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        var consumeLatencies = new List<long>(count);
        using (var consumer = new ConsumerBuilder<Ignore, byte[]>(consumerConfig).Build())
        {
            consumer.Subscribe(topicProduce);
            var sw = new Stopwatch();
            var consumed = 0;

            while (consumed < count)
            {
                sw.Restart();
                var result = consumer.Consume(TimeSpan.FromSeconds(5));
                sw.Stop();

                if (result != null)
                {
                    consumeLatencies.Add(sw.ElapsedTicks);
                    consumed++;
                }
            }
        }

        // End-to-end latency (produce + consume per message)
        Console.WriteLine("  Measuring end-to-end latency...");
        var e2eCount = Math.Min(count, 2000); // Limit E2E due to overhead
        var e2eLatencies = new long[e2eCount];

        using var e2eProducer = new ProducerBuilder<Null, byte[]>(producerConfig).Build();
        var e2eConsumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = $"e2e-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };
        using var e2eConsumer = new ConsumerBuilder<Ignore, byte[]>(e2eConsumerConfig).Build();
        e2eConsumer.Subscribe(topicE2e);

        var e2eSw = new Stopwatch();
        for (int i = 0; i < e2eCount; i++)
        {
            e2eSw.Restart();
            await e2eProducer.ProduceAsync(topicE2e, new Message<Null, byte[]> { Value = payload });
            e2eProducer.Flush(TimeSpan.FromSeconds(1));
            _ = e2eConsumer.Consume(TimeSpan.FromSeconds(5));
            e2eSw.Stop();
            e2eLatencies[i] = e2eSw.ElapsedTicks;
        }

        Array.Sort(produceLatencies);
        consumeLatencies.Sort();
        Array.Sort(e2eLatencies);

        return new LatencyResult
        {
            ProduceP50 = GetPercentile(produceLatencies, 50) / tpm,
            ProduceP99 = GetPercentile(produceLatencies, 99) / tpm,
            ProduceP999 = GetPercentile(produceLatencies, 99.9) / tpm,
            ProduceP9999 = GetPercentile(produceLatencies, 99.99) / tpm,
            ConsumeP50 = GetPercentile(consumeLatencies, 50) / tpm,
            ConsumeP99 = GetPercentile(consumeLatencies, 99) / tpm,
            ConsumeP999 = GetPercentile(consumeLatencies, 99.9) / tpm,
            ConsumeP9999 = GetPercentile(consumeLatencies, 99.99) / tpm,
            E2eP50 = GetPercentile(e2eLatencies, 50) / tpm,
            E2eP99 = GetPercentile(e2eLatencies, 99) / tpm,
            E2eP999 = GetPercentile(e2eLatencies, 99.9) / tpm,
            E2eP9999 = GetPercentile(e2eLatencies, 99.99) / tpm
        };
    }

    private static async Task<LatencyResult> MeasureNativeLatencyAsync(string host, int port, int count, byte[] payload)
    {
        var topicProduce = $"latency-native-produce-{Guid.NewGuid():N}";
        var topicE2e = $"latency-native-e2e-{Guid.NewGuid():N}";
        var tpm = Stopwatch.Frequency / 1_000_000.0;

        await using var client = new SurgewaveNativeClient(host, port);
        await client.ConnectAsync();
        await client.Topics.CreateAsync(topicProduce, 1);
        await client.Topics.CreateAsync(topicE2e, 1);

        // Warmup
        Console.WriteLine("  Warming up...");
        await using var warmupProducer = new SurgewaveBatchingProducer(client, topicProduce, 0, maxBatchSize: 100);
        for (int i = 0; i < Math.Min(100, count / 10); i++)
        {
            await warmupProducer.ProduceAsync(null, payload);
        }
        await warmupProducer.FlushAsync();

        // Producer latency (single message, no batching)
        Console.WriteLine("  Measuring producer latency...");
        await using var producer = new SurgewaveBatchingProducer(client, topicProduce, 0, maxBatchSize: 1, lingerTime: TimeSpan.Zero);

        var produceLatencies = new long[count];
        var sw = new Stopwatch();
        for (int i = 0; i < count; i++)
        {
            sw.Restart();
            await producer.ProduceAsync(null, payload);
            await producer.FlushAsync();
            sw.Stop();
            produceLatencies[i] = sw.ElapsedTicks;
        }

        // Consumer latency
        Console.WriteLine("  Measuring consumer latency...");
        var consumeLatencies = new List<long>(count);
        long offset = 0;
        var consumed = 0;

        while (consumed < count)
        {
            sw.Restart();
            var result = await client.Messaging.ReceiveAsync(topicProduce, 0, offset, 64 * 1024);
            sw.Stop();

            if (result.Messages.Count > 0)
            {
                var perMsg = sw.ElapsedTicks / result.Messages.Count;
                for (int i = 0; i < result.Messages.Count && consumed < count; i++)
                {
                    consumeLatencies.Add(perMsg);
                    consumed++;
                }
                offset = result.Messages[^1].Offset + 1;
            }
            else
            {
                await Task.Delay(5);
            }
        }

        // End-to-end latency
        Console.WriteLine("  Measuring end-to-end latency...");
        var e2eCount = Math.Min(count, 2000);
        var e2eLatencies = new long[e2eCount];
        await using var e2eProducer = new SurgewaveBatchingProducer(client, topicE2e, 0, maxBatchSize: 1, lingerTime: TimeSpan.Zero);
        long e2eOffset = 0;

        for (int i = 0; i < e2eCount; i++)
        {
            sw.Restart();
            await e2eProducer.ProduceAsync(null, payload);
            await e2eProducer.FlushAsync();

            while (true)
            {
                var result = await client.Messaging.ReceiveAsync(topicE2e, 0, e2eOffset, 64 * 1024);
                if (result.Messages.Count > 0)
                {
                    e2eOffset = result.Messages[^1].Offset + 1;
                    break;
                }
                await Task.Delay(1);
            }
            sw.Stop();
            e2eLatencies[i] = sw.ElapsedTicks;
        }

        Array.Sort(produceLatencies);
        consumeLatencies.Sort();
        Array.Sort(e2eLatencies);

        return new LatencyResult
        {
            ProduceP50 = GetPercentile(produceLatencies, 50) / tpm,
            ProduceP99 = GetPercentile(produceLatencies, 99) / tpm,
            ProduceP999 = GetPercentile(produceLatencies, 99.9) / tpm,
            ProduceP9999 = GetPercentile(produceLatencies, 99.99) / tpm,
            ConsumeP50 = GetPercentile(consumeLatencies, 50) / tpm,
            ConsumeP99 = GetPercentile(consumeLatencies, 99) / tpm,
            ConsumeP999 = GetPercentile(consumeLatencies, 99.9) / tpm,
            ConsumeP9999 = GetPercentile(consumeLatencies, 99.99) / tpm,
            E2eP50 = GetPercentile(e2eLatencies, 50) / tpm,
            E2eP99 = GetPercentile(e2eLatencies, 99) / tpm,
            E2eP999 = GetPercentile(e2eLatencies, 99.9) / tpm,
            E2eP9999 = GetPercentile(e2eLatencies, 99.99) / tpm
        };
    }

    private static void PrintLatencyResult(string label, LatencyResult result)
    {
        Console.WriteLine($"  {label} Latencies (µs):");
        Console.WriteLine($"    Produce: P50={result.ProduceP50:F0} P99={result.ProduceP99:F0} P99.9={result.ProduceP999:F0} P99.99={result.ProduceP9999:F0}");
        Console.WriteLine($"    Consume: P50={result.ConsumeP50:F0} P99={result.ConsumeP99:F0} P99.9={result.ConsumeP999:F0} P99.99={result.ConsumeP9999:F0}");
        Console.WriteLine($"    E2E:     P50={result.E2eP50:F0} P99={result.E2eP99:F0} P99.9={result.E2eP999:F0} P99.99={result.E2eP9999:F0}");
    }

    private static void PrintComparisonTable(LatencyResult? kafka, LatencyResult? surgewaveKafka, LatencyResult? surgewaveNative)
    {
        Console.WriteLine("┌─────────────────────┬──────────────────────────────────────────────────────────────────────────┐");
        Console.WriteLine("│                     │                        LATENCY (microseconds)                           │");
        Console.WriteLine("│                     ├───────────────────────┬───────────────────────┬────────────────────────┤");
        Console.WriteLine("│    Operation        │      Pure Kafka       │    Surgewave + Kafka      │    Surgewave Native        │");
        Console.WriteLine("│                     │  P50     P99    P99.9 │  P50     P99    P99.9 │  P50     P99    P99.9  │");
        Console.WriteLine("├─────────────────────┼───────────────────────┼───────────────────────┼────────────────────────┤");

        // Producer
        PrintRow("Producer",
            kafka != null ? (kafka.ProduceP50, kafka.ProduceP99, kafka.ProduceP999) : null,
            surgewaveKafka != null ? (surgewaveKafka.ProduceP50, surgewaveKafka.ProduceP99, surgewaveKafka.ProduceP999) : null,
            surgewaveNative != null ? (surgewaveNative.ProduceP50, surgewaveNative.ProduceP99, surgewaveNative.ProduceP999) : null);

        // Consumer
        PrintRow("Consumer",
            kafka != null ? (kafka.ConsumeP50, kafka.ConsumeP99, kafka.ConsumeP999) : null,
            surgewaveKafka != null ? (surgewaveKafka.ConsumeP50, surgewaveKafka.ConsumeP99, surgewaveKafka.ConsumeP999) : null,
            surgewaveNative != null ? (surgewaveNative.ConsumeP50, surgewaveNative.ConsumeP99, surgewaveNative.ConsumeP999) : null);

        // End-to-End
        PrintRow("End-to-End",
            kafka != null ? (kafka.E2eP50, kafka.E2eP99, kafka.E2eP999) : null,
            surgewaveKafka != null ? (surgewaveKafka.E2eP50, surgewaveKafka.E2eP99, surgewaveKafka.E2eP999) : null,
            surgewaveNative != null ? (surgewaveNative.E2eP50, surgewaveNative.E2eP99, surgewaveNative.E2eP999) : null);

        Console.WriteLine("└─────────────────────┴───────────────────────┴───────────────────────┴────────────────────────┘");

        // Summary comparison
        if (kafka != null && surgewaveNative != null)
        {
            Console.WriteLine();
            Console.WriteLine("Performance vs Pure Kafka (P99):");
            Console.WriteLine($"  Surgewave Native Producer:  {kafka.ProduceP99 / surgewaveNative.ProduceP99:F1}x faster");
            Console.WriteLine($"  Surgewave Native Consumer:  {kafka.ConsumeP99 / surgewaveNative.ConsumeP99:F1}x faster");
            Console.WriteLine($"  Surgewave Native E2E:       {kafka.E2eP99 / surgewaveNative.E2eP99:F1}x faster");
        }

        if (kafka != null && surgewaveKafka != null)
        {
            Console.WriteLine();
            Console.WriteLine("Surgewave+Kafka Protocol vs Pure Kafka (P99):");
            Console.WriteLine($"  Producer: {kafka.ProduceP99 / surgewaveKafka.ProduceP99:F1}x faster");
            Console.WriteLine($"  Consumer: {kafka.ConsumeP99 / surgewaveKafka.ConsumeP99:F1}x faster");
            Console.WriteLine($"  E2E:      {kafka.E2eP99 / surgewaveKafka.E2eP99:F1}x faster");
        }
    }

    private static void PrintRow(string label, (double p50, double p99, double p999)? kafka,
        (double p50, double p99, double p999)? surgewaveKafka, (double p50, double p99, double p999)? surgewaveNative)
    {
        var kafkaStr = kafka.HasValue
            ? $"{kafka.Value.p50,6:F0} {kafka.Value.p99,7:F0} {kafka.Value.p999,8:F0}"
            : "      N/A       N/A      N/A";
        var surgewaveKafkaStr = surgewaveKafka.HasValue
            ? $"{surgewaveKafka.Value.p50,6:F0} {surgewaveKafka.Value.p99,7:F0} {surgewaveKafka.Value.p999,8:F0}"
            : "      N/A       N/A      N/A";
        var surgewaveNativeStr = surgewaveNative.HasValue
            ? $"{surgewaveNative.Value.p50,6:F0} {surgewaveNative.Value.p99,7:F0} {surgewaveNative.Value.p999,8:F0}"
            : "      N/A       N/A      N/A";

        Console.WriteLine($"│ {label,-19} │ {kafkaStr} │ {surgewaveKafkaStr} │ {surgewaveNativeStr}  │");
    }

    private static long GetPercentile(long[] sortedData, double percentile)
    {
        if (sortedData.Length == 0) return 0;
        var index = (percentile / 100.0) * (sortedData.Length - 1);
        var lower = (int)Math.Floor(index);
        return sortedData[Math.Min(lower, sortedData.Length - 1)];
    }

    private static double GetPercentile(List<long> sortedData, double percentile)
    {
        if (sortedData.Count == 0) return 0;
        var index = (percentile / 100.0) * (sortedData.Count - 1);
        var lower = (int)Math.Floor(index);
        return sortedData[Math.Min(lower, sortedData.Count - 1)];
    }

    private sealed record LatencyResult
    {
        public double ProduceP50 { get; init; }
        public double ProduceP99 { get; init; }
        public double ProduceP999 { get; init; }
        public double ProduceP9999 { get; init; }
        public double ConsumeP50 { get; init; }
        public double ConsumeP99 { get; init; }
        public double ConsumeP999 { get; init; }
        public double ConsumeP9999 { get; init; }
        public double E2eP50 { get; init; }
        public double E2eP99 { get; init; }
        public double E2eP999 { get; init; }
        public double E2eP9999 { get; init; }
    }
}
