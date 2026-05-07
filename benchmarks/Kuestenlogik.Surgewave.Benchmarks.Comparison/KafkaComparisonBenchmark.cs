using System.Diagnostics;
using Confluent.Kafka;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Client.Native.Operations.Performance;
using Kuestenlogik.Surgewave.Runtime;
using Testcontainers.Kafka;

namespace Kuestenlogik.Surgewave.Benchmarks.Comparison;

/// <summary>
/// Comprehensive benchmark comparing Surgewave against real Apache Kafka using Testcontainers.
/// Tests different Surgewave storage backends against Kafka for latency and throughput.
/// </summary>
public static class KafkaComparisonBenchmark
{
    public static async Task RunAsync(string[] args)
    {
        var messageCount = args.Length > 0 ? int.Parse(args[0]) : 10000;
        var messageSize = args.Length > 1 ? int.Parse(args[1]) : 100;

        Console.WriteLine("╔══════════════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║        Surgewave vs REAL APACHE KAFKA COMPARISON (Testcontainers)           ║");
        Console.WriteLine("╚══════════════════════════════════════════════════════════════════════════╝");
        Console.WriteLine($"  Messages:     {messageCount:N0}");
        Console.WriteLine($"  Message size: {messageSize} bytes");
        Console.WriteLine();

        var payload = new byte[messageSize];
        Random.Shared.NextBytes(payload);

        // Results storage
        var results = new List<BenchmarkResult>();

        // ═══════════════════════════════════════════════════════════════
        // REAL APACHE KAFKA (via Testcontainers)
        // ═══════════════════════════════════════════════════════════════
        Console.WriteLine("Starting Apache Kafka via Testcontainers...");
        await using var kafkaContainer = new KafkaBuilder("confluentinc/cp-kafka:7.6.0")
            .Build();

        await kafkaContainer.StartAsync();
        var kafkaBootstrap = kafkaContainer.GetBootstrapAddress();
        Console.WriteLine($"Kafka running at: {kafkaBootstrap}");
        Console.WriteLine();

        Console.WriteLine("═══════════════════════════════════════════════════════════════════════════");
        Console.WriteLine("  APACHE KAFKA (Real Broker)");
        Console.WriteLine("═══════════════════════════════════════════════════════════════════════════");

        var kafkaResult = await RunKafkaBenchmarkAsync(kafkaBootstrap, messageCount, payload);
        results.Add(kafkaResult);
        PrintResult(kafkaResult);

        // ═══════════════════════════════════════════════════════════════
        // Surgewave WITH DIFFERENT STORAGE BACKENDS
        // ═══════════════════════════════════════════════════════════════
        var storageBackends = new[]
        {
            (Engine: StorageEngines.Memory, Name: "Memory"),
            (Engine: StorageEngines.File, Name: "File"),
            (Engine: StorageEngines.ZeroCopyWal, Name: "ZeroCopyWal"),
        };

        foreach (var (engine, name) in storageBackends)
        {
            Console.WriteLine();
            Console.WriteLine("═══════════════════════════════════════════════════════════════════════════");
            Console.WriteLine($"  Surgewave ({name} Storage)");
            Console.WriteLine("═══════════════════════════════════════════════════════════════════════════");

            await using var surgewave = await SurgewaveRuntime.CreateBuilder()
                .WithPort(0)
                .WithAutoCreateTopics(true)
                .WithPartitions(1)
                .WithStorageEngine(engine)
                .Build()
                .StartAsync();

            var surgewaveResult = await RunSurgewaveBenchmarkAsync(surgewave.Host, surgewave.Port, messageCount, payload, name);
            results.Add(surgewaveResult);
            PrintResult(surgewaveResult);
        }

        // ═══════════════════════════════════════════════════════════════
        // COMPARISON SUMMARY
        // ═══════════════════════════════════════════════════════════════
        Console.WriteLine();
        PrintComparisonSummary(results);
    }

    private static async Task<BenchmarkResult> RunKafkaBenchmarkAsync(string bootstrap, int count, byte[] payload)
    {
        var topic = $"kafka-bench-{Guid.NewGuid():N}";
        var tpm = Stopwatch.Frequency / 1_000_000.0;

        // Warmup
        Console.WriteLine("  Warming up...");
        var warmupConfig = new ProducerConfig { BootstrapServers = bootstrap };
        using (var warmup = new ProducerBuilder<Null, byte[]>(warmupConfig).Build())
        {
            for (int i = 0; i < Math.Min(500, count / 10); i++)
            {
                await warmup.ProduceAsync(topic, new Message<Null, byte[]> { Value = payload });
            }
            warmup.Flush(TimeSpan.FromSeconds(10));
        }

        // Producer latency
        Console.WriteLine("  Measuring producer latency...");
        var producerConfig = new ProducerConfig { BootstrapServers = bootstrap, Acks = Acks.All };
        var produceLatencies = new long[count];

        using (var producer = new ProducerBuilder<Null, byte[]>(producerConfig).Build())
        {
            var sw = new Stopwatch();
            for (int i = 0; i < count; i++)
            {
                sw.Restart();
                await producer.ProduceAsync(topic, new Message<Null, byte[]> { Value = payload });
                sw.Stop();
                produceLatencies[i] = sw.ElapsedTicks;
            }
            producer.Flush(TimeSpan.FromSeconds(10));
        }

        // Consumer latency
        Console.WriteLine("  Measuring consumer latency...");
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrap,
            GroupId = $"bench-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        var consumeLatencies = new List<long>(count);
        using (var consumer = new ConsumerBuilder<Ignore, byte[]>(consumerConfig).Build())
        {
            consumer.Subscribe(topic);
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

        // Throughput (batched producer)
        Console.WriteLine("  Measuring throughput...");
        var throughputConfig = new ProducerConfig
        {
            BootstrapServers = bootstrap,
            Acks = Acks.All,
            LingerMs = 5,
            BatchSize = 16384
        };

        var throughputTopic = $"kafka-throughput-{Guid.NewGuid():N}";
        using var throughputProducer = new ProducerBuilder<Null, byte[]>(throughputConfig).Build();

        var throughputSw = Stopwatch.StartNew();
        for (int i = 0; i < count; i++)
        {
            throughputProducer.Produce(throughputTopic, new Message<Null, byte[]> { Value = payload });
        }
        throughputProducer.Flush(TimeSpan.FromSeconds(30));
        throughputSw.Stop();
        var produceRate = count / throughputSw.Elapsed.TotalSeconds;

        // Consumer throughput
        var consumeThroughputConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrap,
            GroupId = $"throughput-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            FetchMinBytes = 1,
            FetchMaxBytes = 52428800
        };

        using var consumeThroughput = new ConsumerBuilder<Ignore, byte[]>(consumeThroughputConfig).Build();
        consumeThroughput.Subscribe(throughputTopic);

        var consumeSwThroughput = Stopwatch.StartNew();
        var totalConsumed = 0;
        while (totalConsumed < count)
        {
            var result = consumeThroughput.Consume(TimeSpan.FromSeconds(10));
            if (result != null) totalConsumed++;
        }
        consumeSwThroughput.Stop();
        var consumeRate = count / consumeSwThroughput.Elapsed.TotalSeconds;

        Array.Sort(produceLatencies);
        consumeLatencies.Sort();

        return new BenchmarkResult
        {
            Name = "Apache Kafka",
            ProduceP50 = GetPercentile(produceLatencies, 50) / tpm,
            ProduceP99 = GetPercentile(produceLatencies, 99) / tpm,
            ProduceP999 = GetPercentile(produceLatencies, 99.9) / tpm,
            ProduceP9999 = GetPercentile(produceLatencies, 99.99) / tpm,
            ConsumeP50 = GetPercentile(consumeLatencies, 50) / tpm,
            ConsumeP99 = GetPercentile(consumeLatencies, 99) / tpm,
            ConsumeP999 = GetPercentile(consumeLatencies, 99.9) / tpm,
            ConsumeP9999 = GetPercentile(consumeLatencies, 99.99) / tpm,
            ProduceThroughput = produceRate,
            ConsumeThroughput = consumeRate
        };
    }

    private static async Task<BenchmarkResult> RunSurgewaveBenchmarkAsync(string host, int port, int count, byte[] payload, string storageName)
    {
        var topic = $"surgewave-bench-{Guid.NewGuid():N}";
        var tpm = Stopwatch.Frequency / 1_000_000.0;

        await using var client = new SurgewaveNativeClient(host, port);
        await client.ConnectAsync();
        await client.Topics.CreateAsync(topic, 1);

        // Warmup
        Console.WriteLine("  Warming up...");
        await using (var warmup = new SurgewaveBatchingProducer(client, topic, 0, maxBatchSize: 100))
        {
            for (int i = 0; i < Math.Min(500, count / 10); i++)
            {
                await warmup.ProduceAsync(null, payload);
            }
            await warmup.FlushAsync();
        }

        // Producer latency (single message, no batching)
        Console.WriteLine("  Measuring producer latency...");
        var produceLatencies = new long[count];
        await using (var producer = new SurgewaveBatchingProducer(client, topic, 0, maxBatchSize: 1, lingerTime: TimeSpan.Zero))
        {
            var sw = new Stopwatch();
            for (int i = 0; i < count; i++)
            {
                sw.Restart();
                await producer.ProduceAsync(null, payload);
                await producer.FlushAsync();
                sw.Stop();
                produceLatencies[i] = sw.ElapsedTicks;
            }
        }

        // Consumer latency (with prefetching for fair comparison)
        Console.WriteLine("  Measuring consumer latency...");
        await using var consumer = new SurgewavePrefetchingConsumer(
            client, topic, 0, 0, count + 1000, 1024 * 1024);
        await Task.Delay(200); // Let prefetcher warm up

        var consumeLatencies = new long[count];
        var sw2 = new Stopwatch();
        for (int i = 0; i < count; i++)
        {
            sw2.Restart();
            var msg = consumer.Consume() ?? await consumer.ConsumeAsync();
            sw2.Stop();
            consumeLatencies[i] = sw2.ElapsedTicks;
        }

        // Throughput (batched)
        Console.WriteLine("  Measuring throughput...");
        var throughputTopic = $"surgewave-throughput-{Guid.NewGuid():N}";
        await client.Topics.CreateAsync(throughputTopic, 1);

        await using var throughputProducer = new SurgewaveBatchingProducer(
            client, throughputTopic, 0, maxBatchSize: 1000, lingerTime: TimeSpan.FromMilliseconds(5));

        var throughputSw = Stopwatch.StartNew();
        for (int i = 0; i < count; i++)
        {
            await throughputProducer.ProduceAsync(null, payload);
        }
        await throughputProducer.FlushAsync();
        throughputSw.Stop();
        var produceRate = count / throughputSw.Elapsed.TotalSeconds;

        // Consumer throughput
        await using var consumeThroughput = new SurgewavePrefetchingConsumer(
            client, throughputTopic, 0, 0, count + 1000, 1024 * 1024);
        await Task.Delay(500); // Let prefetcher buffer

        var consumeSwThroughput = Stopwatch.StartNew();
        var totalConsumed = 0;
        while (totalConsumed < count)
        {
            var msg = consumeThroughput.Consume() ?? await consumeThroughput.ConsumeAsync();
            if (msg != null) totalConsumed++;
        }
        consumeSwThroughput.Stop();
        var consumeRate = count / consumeSwThroughput.Elapsed.TotalSeconds;

        Array.Sort(produceLatencies);
        Array.Sort(consumeLatencies);

        return new BenchmarkResult
        {
            Name = $"Surgewave ({storageName})",
            ProduceP50 = GetPercentile(produceLatencies, 50) / tpm,
            ProduceP99 = GetPercentile(produceLatencies, 99) / tpm,
            ProduceP999 = GetPercentile(produceLatencies, 99.9) / tpm,
            ProduceP9999 = GetPercentile(produceLatencies, 99.99) / tpm,
            ConsumeP50 = GetPercentile(consumeLatencies, 50) / tpm,
            ConsumeP99 = GetPercentile(consumeLatencies, 99) / tpm,
            ConsumeP999 = GetPercentile(consumeLatencies, 99.9) / tpm,
            ConsumeP9999 = GetPercentile(consumeLatencies, 99.99) / tpm,
            ProduceThroughput = produceRate,
            ConsumeThroughput = consumeRate
        };
    }

    private static void PrintResult(BenchmarkResult result)
    {
        Console.WriteLine($"  Latency (µs):    P50={result.ProduceP50:F0}  P99={result.ProduceP99:F0}  P99.9={result.ProduceP999:F0}  P99.99={result.ProduceP9999:F0}");
        Console.WriteLine($"  Consume (µs):    P50={result.ConsumeP50:F1}  P99={result.ConsumeP99:F1}  P99.9={result.ConsumeP999:F1}  P99.99={result.ConsumeP9999:F1}");
        Console.WriteLine($"  Throughput:      Produce={result.ProduceThroughput:N0} msg/s  Consume={result.ConsumeThroughput:N0} msg/s");
    }

    private static void PrintComparisonSummary(List<BenchmarkResult> results)
    {
        Console.WriteLine("╔══════════════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║                        COMPARISON SUMMARY                                ║");
        Console.WriteLine("╚══════════════════════════════════════════════════════════════════════════╝");
        Console.WriteLine();

        // Latency table
        Console.WriteLine("PRODUCER LATENCY (microseconds):");
        Console.WriteLine("┌─────────────────────────┬──────────┬──────────┬──────────┬──────────┐");
        Console.WriteLine("│ System                  │    P50   │    P99   │   P99.9  │  P99.99  │");
        Console.WriteLine("├─────────────────────────┼──────────┼──────────┼──────────┼──────────┤");
        foreach (var r in results)
        {
            Console.WriteLine($"│ {r.Name,-23} │ {r.ProduceP50,8:F0} │ {r.ProduceP99,8:F0} │ {r.ProduceP999,8:F0} │ {r.ProduceP9999,8:F0} │");
        }
        Console.WriteLine("└─────────────────────────┴──────────┴──────────┴──────────┴──────────┘");

        Console.WriteLine();
        Console.WriteLine("CONSUMER LATENCY (microseconds):");
        Console.WriteLine("┌─────────────────────────┬──────────┬──────────┬──────────┬──────────┐");
        Console.WriteLine("│ System                  │    P50   │    P99   │   P99.9  │  P99.99  │");
        Console.WriteLine("├─────────────────────────┼──────────┼──────────┼──────────┼──────────┤");
        foreach (var r in results)
        {
            Console.WriteLine($"│ {r.Name,-23} │ {r.ConsumeP50,8:F1} │ {r.ConsumeP99,8:F1} │ {r.ConsumeP999,8:F1} │ {r.ConsumeP9999,8:F1} │");
        }
        Console.WriteLine("└─────────────────────────┴──────────┴──────────┴──────────┴──────────┘");

        Console.WriteLine();
        Console.WriteLine("THROUGHPUT (messages/second):");
        Console.WriteLine("┌─────────────────────────┬────────────────┬────────────────┐");
        Console.WriteLine("│ System                  │    Producer    │    Consumer    │");
        Console.WriteLine("├─────────────────────────┼────────────────┼────────────────┤");
        foreach (var r in results)
        {
            Console.WriteLine($"│ {r.Name,-23} │ {r.ProduceThroughput,14:N0} │ {r.ConsumeThroughput,14:N0} │");
        }
        Console.WriteLine("└─────────────────────────┴────────────────┴────────────────┘");

        // Comparison vs Kafka
        var kafka = results.FirstOrDefault(r => r.Name == "Apache Kafka");
        if (kafka != null)
        {
            Console.WriteLine();
            Console.WriteLine("PERFORMANCE vs APACHE KAFKA:");
            Console.WriteLine("┌─────────────────────────┬───────────────────┬───────────────────┐");
            Console.WriteLine("│ System                  │  Produce P99      │  Throughput       │");
            Console.WriteLine("├─────────────────────────┼───────────────────┼───────────────────┤");
            foreach (var r in results.Where(r => r.Name != "Apache Kafka"))
            {
                var latencyRatio = kafka.ProduceP99 / r.ProduceP99;
                var throughputRatio = r.ProduceThroughput / kafka.ProduceThroughput;
                var latencyStr = latencyRatio > 1 ? $"{latencyRatio:F0}x faster" : $"{1/latencyRatio:F1}x slower";
                var throughputStr = throughputRatio > 1 ? $"{throughputRatio:F1}x higher" : $"{1/throughputRatio:F1}x lower";
                Console.WriteLine($"│ {r.Name,-23} │ {latencyStr,-17} │ {throughputStr,-17} │");
            }
            Console.WriteLine("└─────────────────────────┴───────────────────┴───────────────────┘");
        }
    }

    private static long GetPercentile(long[] sortedData, double percentile)
    {
        if (sortedData.Length == 0) return 0;
        var index = (percentile / 100.0) * (sortedData.Length - 1);
        return sortedData[Math.Min((int)index, sortedData.Length - 1)];
    }

    private static double GetPercentile(List<long> sortedData, double percentile)
    {
        if (sortedData.Count == 0) return 0;
        var index = (percentile / 100.0) * (sortedData.Count - 1);
        return sortedData[Math.Min((int)index, sortedData.Count - 1)];
    }

    private sealed record BenchmarkResult
    {
        public required string Name { get; init; }
        public double ProduceP50 { get; init; }
        public double ProduceP99 { get; init; }
        public double ProduceP999 { get; init; }
        public double ProduceP9999 { get; init; }
        public double ConsumeP50 { get; init; }
        public double ConsumeP99 { get; init; }
        public double ConsumeP999 { get; init; }
        public double ConsumeP9999 { get; init; }
        public double ProduceThroughput { get; init; }
        public double ConsumeThroughput { get; init; }
    }
}
