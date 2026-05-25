using System.Diagnostics;
using System.Text;
using Confluent.Kafka;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Client.Native.Operations.Performance;
using Kuestenlogik.Surgewave.Runtime;

namespace Kuestenlogik.Surgewave.Benchmarks.Comparison;

/// <summary>
/// Embedded protocol comparison benchmark.
/// Compares Kafka client (Confluent.Kafka) vs Surgewave Native client against embedded broker.
/// Self-contained - no external broker required.
///
/// Run with: dotnet run -- embedded-compare [msgCount] [msgSize] [batchSize]
/// </summary>
public static class EmbeddedProtocolComparisonBenchmark
{
    public static async Task RunAsync(string[] args)
    {
        var messageCount = args.Length > 0 ? int.Parse(args[0]) : 100_000;
        var messageSizeBytes = args.Length > 1 ? int.Parse(args[1]) : 100;
        var batchSize = args.Length > 2 ? int.Parse(args[2]) : 1000;

        Console.WriteLine("Embedded Protocol Comparison Benchmark");
        Console.WriteLine("======================================");
        Console.WriteLine("Compares Kafka Wire Protocol vs Surgewave Native Protocol");
        Console.WriteLine("on the same embedded broker (self-contained, no external broker)");
        Console.WriteLine();
        Console.WriteLine($"Messages:     {messageCount:N0}");
        Console.WriteLine($"Message size: {messageSizeBytes} bytes");
        Console.WriteLine($"Batch size:   {batchSize}");
        Console.WriteLine($"Total data:   {(long)messageCount * messageSizeBytes / 1024 / 1024:N1} MB");
        Console.WriteLine();

        // Start embedded broker
        Console.WriteLine("Starting embedded broker...");
        var startupSw = Stopwatch.StartNew();
        await using var surgewave = await SurgewaveRuntime.CreateBuilder()
            .WithPort(0) // Auto-assign port
            .WithAutoCreateTopics(true)
            .WithPartitions(1)
            .Build()
            .StartAsync();
        startupSw.Stop();
        Console.WriteLine($"Embedded broker started in {startupSw.ElapsedMilliseconds} ms");
        Console.WriteLine($"Listening on port: {surgewave.Port}");
        Console.WriteLine();

        // Prepare test data
        var messageValue = new byte[messageSizeBytes];
        Random.Shared.NextBytes(messageValue);
        var bootstrapServers = surgewave.BootstrapServers;

        // Run Surgewave Native Protocol benchmark
        Console.WriteLine("═══════════════════════════════════════════════════════════════════");
        Console.WriteLine("  Surgewave NATIVE PROTOCOL (SurgewaveNativeClient)");
        Console.WriteLine("═══════════════════════════════════════════════════════════════════");
        var nativeResults = await RunNativeProtocolBenchmarkAsync(
            surgewave.Host, surgewave.Port, messageCount, messageSizeBytes, batchSize, messageValue);

        // Run Kafka Wire Protocol benchmark
        Console.WriteLine();
        Console.WriteLine("═══════════════════════════════════════════════════════════════════");
        Console.WriteLine("  KAFKA WIRE PROTOCOL (Confluent.Kafka client)");
        Console.WriteLine("═══════════════════════════════════════════════════════════════════");
        var kafkaResults = await RunKafkaProtocolBenchmarkAsync(
            bootstrapServers, messageCount, messageSizeBytes, batchSize, messageValue);

        // Print comparison
        Console.WriteLine();
        Console.WriteLine("═══════════════════════════════════════════════════════════════════");
        Console.WriteLine("  PROTOCOL COMPARISON SUMMARY");
        Console.WriteLine("═══════════════════════════════════════════════════════════════════");
        Console.WriteLine();
        Console.WriteLine($"{"Metric",-32} {"Native Protocol",-18} {"Kafka Protocol",-18} {"Delta",-12}");
        Console.WriteLine(new string('-', 82));

        PrintComparisonRow("Batched Producer (msg/sec)", nativeResults.ProducerMsgPerSec, kafkaResults.ProducerMsgPerSec);
        PrintComparisonRow("Batched Producer (MB/sec)", nativeResults.ProducerMBPerSec, kafkaResults.ProducerMBPerSec);
        PrintComparisonRow("Consumer (msg/sec)", nativeResults.ConsumerMsgPerSec, kafkaResults.ConsumerMsgPerSec);
        PrintComparisonRow("Consumer (MB/sec)", nativeResults.ConsumerMBPerSec, kafkaResults.ConsumerMBPerSec);

        Console.WriteLine();
        Console.WriteLine("Note: Both protocols tested against identical embedded broker.");
        Console.WriteLine("      Native protocol is optimized binary, Kafka is wire-compatible.");
    }

    private static void PrintComparisonRow(string metric, double native, double kafka)
    {
        var delta = (native - kafka) / kafka * 100;
        var deltaStr = delta >= 0 ? $"+{delta:N1}%" : $"{delta:N1}%";
        var winner = native > kafka ? "Native" : "Kafka";
        Console.WriteLine($"{metric,-32} {native,16:N0} {kafka,16:N0} {deltaStr,-10} ({winner})");
    }

    private static async Task<BenchmarkResults> RunNativeProtocolBenchmarkAsync(
        string host, int port, int messageCount, int messageSize, int batchSize, byte[] messageValue)
    {
        var topicName = $"native-proto-bench-{Guid.NewGuid():N}";

        await using var client = new SurgewaveNativeClient(host, port);
        await client.ConnectAsync();

        // Create topic
        await client.Topics.CreateAsync(topicName, 1);

        // Producer benchmark with batching
        Console.WriteLine("  [Batched Producer Test]");
        await using var producer = new SurgewaveBatchingProducer(
            client,
            topicName,
            partition: 0,
            maxBatchSize: batchSize,
            lingerTime: TimeSpan.FromMilliseconds(5));

        var sw = Stopwatch.StartNew();
        for (int i = 0; i < messageCount; i++)
        {
            await producer.ProduceAsync(null, messageValue);

            if (i > 0 && i % 100_000 == 0)
            {
                Console.WriteLine($"    Produced {i:N0} messages...");
            }
        }
        await producer.FlushAsync();
        sw.Stop();

        var produceMs = sw.ElapsedMilliseconds;
        var produceMsgPerSec = messageCount * 1000.0 / produceMs;
        var produceMBPerSec = (long)messageCount * messageSize / 1024.0 / 1024.0 * 1000.0 / produceMs;
        Console.WriteLine($"    Time: {produceMs:N0} ms");
        Console.WriteLine($"    Throughput: {produceMsgPerSec:N0} msg/sec ({produceMBPerSec:N1} MB/sec)");

        // Consumer benchmark using prefetching consumer
        Console.WriteLine("  [Consumer Test] (Prefetching)");

        // Create prefetching consumer - it starts background fetching immediately
        await using var consumer = new SurgewavePrefetchingConsumer(
            client,
            topicName,
            partition: 0,
            startOffset: 0,
            prefetchCount: 100000,  // Large buffer for benchmark
            maxBytesPerFetch: 1024 * 1024);

        // Wait for buffer to start filling
        await consumer.WaitForBufferAsync(Math.Min(10000, messageCount));

        sw.Restart();
        var consumed = 0;

        while (consumed < messageCount)
        {
            var msg = consumer.Consume();
            if (msg == null)
            {
                // Buffer empty, wait for more
                msg = await consumer.ConsumeAsync();
                if (msg == null) break;
            }
            consumed++;

            if (consumed > 0 && consumed % 100_000 == 0)
            {
                Console.WriteLine($"    Consumed {consumed:N0} messages...");
            }
        }
        sw.Stop();

        var consumeMs = sw.ElapsedMilliseconds;
        var consumeMsgPerSec = consumed * 1000.0 / consumeMs;
        var consumeMBPerSec = (long)consumed * messageSize / 1024.0 / 1024.0 * 1000.0 / consumeMs;
        Console.WriteLine($"    Messages: {consumed:N0}");
        Console.WriteLine($"    Time: {consumeMs:N0} ms");
        Console.WriteLine($"    Throughput: {consumeMsgPerSec:N0} msg/sec ({consumeMBPerSec:N1} MB/sec)");

        return new BenchmarkResults(produceMsgPerSec, produceMBPerSec, consumeMsgPerSec, consumeMBPerSec);
    }

    private static async Task<BenchmarkResults> RunKafkaProtocolBenchmarkAsync(
        string bootstrapServers, int messageCount, int messageSize, int batchSize, byte[] messageValue)
    {
        var topicName = $"kafka-proto-bench-{Guid.NewGuid():N}";

        // Producer benchmark with batching
        Console.WriteLine("  [Batched Producer Test]");
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            Acks = Acks.Leader,
            LingerMs = 5,
            BatchSize = 65536,
            QueueBufferingMaxMessages = 2000000,
            QueueBufferingMaxKbytes = 2097152,
        };

        double producerMsgPerSec, producerMBPerSec;
        using (var producer = new ProducerBuilder<Null, byte[]>(producerConfig).Build())
        {
            var sw = Stopwatch.StartNew();
            for (int i = 0; i < messageCount; i++)
            {
                producer.Produce(topicName, new Message<Null, byte[]> { Value = messageValue });

                if (i > 0 && i % 100_000 == 0)
                {
                    Console.WriteLine($"    Produced {i:N0} messages...");
                }
            }
            producer.Flush(TimeSpan.FromSeconds(60));
            sw.Stop();

            var produceMs = sw.ElapsedMilliseconds;
            producerMsgPerSec = messageCount * 1000.0 / produceMs;
            producerMBPerSec = (long)messageCount * messageSize / 1024.0 / 1024.0 * 1000.0 / produceMs;
            Console.WriteLine($"    Time: {produceMs:N0} ms");
            Console.WriteLine($"    Throughput: {producerMsgPerSec:N0} msg/sec ({producerMBPerSec:N1} MB/sec)");
        }

        // Consumer benchmark
        Console.WriteLine("  [Consumer Test]");
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = $"kafka-proto-consumer-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        };

        double consumerMsgPerSec, consumerMBPerSec;
        using (var consumer = new ConsumerBuilder<Null, byte[]>(consumerConfig).Build())
        {
            consumer.Assign([new TopicPartitionOffset(topicName, 0, Offset.Beginning)]);

            var sw = Stopwatch.StartNew();
            var consumed = 0;
            var noMessageCount = 0;
            const int maxNoMessageRetries = 3;

            while (consumed < messageCount && noMessageCount < maxNoMessageRetries)
            {
                var result = consumer.Consume(TimeSpan.FromSeconds(5));
                if (result == null)
                {
                    noMessageCount++;
                    continue;
                }
                noMessageCount = 0;
                consumed++;

                if (consumed > 0 && consumed % 100_000 == 0)
                {
                    Console.WriteLine($"    Consumed {consumed:N0} messages...");
                }
            }
            sw.Stop();

            var consumeMs = sw.ElapsedMilliseconds;
            consumerMsgPerSec = consumed * 1000.0 / consumeMs;
            consumerMBPerSec = (long)consumed * messageSize / 1024.0 / 1024.0 * 1000.0 / consumeMs;
            Console.WriteLine($"    Messages: {consumed:N0}");
            Console.WriteLine($"    Time: {consumeMs:N0} ms");
            Console.WriteLine($"    Throughput: {consumerMsgPerSec:N0} msg/sec ({consumerMBPerSec:N1} MB/sec)");
        }

        return new BenchmarkResults(producerMsgPerSec, producerMBPerSec, consumerMsgPerSec, consumerMBPerSec);
    }

    private sealed record BenchmarkResults(
        double ProducerMsgPerSec,
        double ProducerMBPerSec,
        double ConsumerMsgPerSec,
        double ConsumerMBPerSec);
}
