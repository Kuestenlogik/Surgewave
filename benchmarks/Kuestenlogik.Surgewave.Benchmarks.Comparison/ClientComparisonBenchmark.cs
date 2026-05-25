using System.Diagnostics;
using System.Text;
using Confluent.Kafka;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Client.Native.Operations.Performance;

namespace Kuestenlogik.Surgewave.Benchmarks.Comparison;

/// <summary>
/// Comparison benchmark between Kafka client (Confluent.Kafka) and Surgewave Native client.
/// Measures throughput for produce and consume operations.
/// </summary>
public static class ClientComparisonBenchmark
{
    private const string BootstrapServers = "localhost:9092";
    private const int DefaultMessageCount = 100_000;
    private const int DefaultMessageSize = 1024;

    public static async Task RunAsync(string[] args)
    {
        var messageCount = args.Length > 0 ? int.Parse(args[0]) : DefaultMessageCount;
        var messageSize = args.Length > 1 ? int.Parse(args[1]) : DefaultMessageSize;

        Console.WriteLine("╔════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║     Surgewave Client Comparison Benchmark                          ║");
        Console.WriteLine("║     Kafka Client (Confluent.Kafka) vs Surgewave Native Client      ║");
        Console.WriteLine("╚════════════════════════════════════════════════════════════════╝");
        Console.WriteLine();
        Console.WriteLine($"Configuration:");
        Console.WriteLine($"  Messages:     {messageCount:N0}");
        Console.WriteLine($"  Message size: {messageSize:N0} bytes");
        Console.WriteLine($"  Total data:   {(long)messageCount * messageSize / 1024 / 1024:N0} MB");
        Console.WriteLine($"  Bootstrap:    {BootstrapServers}");
        Console.WriteLine();

        // Create message payload
        var messageValue = new byte[messageSize];
        Random.Shared.NextBytes(messageValue);

        // Run Kafka client benchmark
        Console.WriteLine("═══════════════════════════════════════════════════════════════════");
        Console.WriteLine("  KAFKA CLIENT (Confluent.Kafka)");
        Console.WriteLine("═══════════════════════════════════════════════════════════════════");
        var kafkaResults = await RunKafkaClientBenchmarkAsync(messageCount, messageValue);

        // Run Surgewave native client benchmark
        Console.WriteLine();
        Console.WriteLine("═══════════════════════════════════════════════════════════════════");
        Console.WriteLine("  Surgewave NATIVE CLIENT");
        Console.WriteLine("═══════════════════════════════════════════════════════════════════");
        var nativeResults = await RunNativeClientBenchmarkAsync(messageCount, messageValue);

        // Print comparison
        Console.WriteLine();
        Console.WriteLine("═══════════════════════════════════════════════════════════════════");
        Console.WriteLine("  COMPARISON SUMMARY");
        Console.WriteLine("═══════════════════════════════════════════════════════════════════");
        Console.WriteLine();
        Console.WriteLine("Note: Sync producer comparison is the fair test - measures protocol overhead");
        Console.WriteLine("      for individual message acknowledgment (same pattern for both clients).");
        Console.WriteLine();
        Console.WriteLine($"{"Metric",-30} {"Kafka Client",-18} {"Surgewave Native",-18} {"Delta",-15}");
        Console.WriteLine(new string('-', 85));

        // Sync producer comparison (fair comparison - both clients do per-message ack)
        PrintComparisonRow("Sync Producer (msg/sec)", kafkaResults.ProducerSyncMsgPerSec, nativeResults.ProducerSyncMsgPerSec);
        PrintComparisonRow("Sync Producer (MB/sec)", kafkaResults.ProducerSyncMBPerSec, nativeResults.ProducerSyncMBPerSec);

        // Consumer comparison
        PrintComparisonRow("Consumer (msg/sec)", kafkaResults.ConsumerMsgPerSec, nativeResults.ConsumerMsgPerSec);
        PrintComparisonRow("Consumer (MB/sec)", kafkaResults.ConsumerMBPerSec, nativeResults.ConsumerMBPerSec);

        // Batched producer comparison
        PrintComparisonRow("Batched Producer (msg/sec)", kafkaResults.ProducerBatchedMsgPerSec, nativeResults.ProducerBatchedMsgPerSec);
        PrintComparisonRow("Batched Producer (MB/sec)", kafkaResults.ProducerBatchedMBPerSec, nativeResults.ProducerBatchedMBPerSec);

        Console.WriteLine();
    }

    private static void PrintComparisonRow(string metric, double kafka, double native)
    {
        var delta = (native - kafka) / kafka * 100;
        var deltaStr = delta >= 0 ? $"+{delta:N1}%" : $"{delta:N1}%";
        var winner = native > kafka ? "Native" : "Kafka";
        Console.WriteLine($"{metric,-30} {kafka,16:N0} {native,16:N0} {deltaStr,-12} ({winner})");
    }

    private static async Task<BenchmarkResults> RunKafkaClientBenchmarkAsync(int messageCount, byte[] messageValue)
    {
        var topicName = $"kafka-bench-{Guid.NewGuid():N}";
        var messageSize = messageValue.Length;

        // Producer test - BATCHED (fire-and-forget with flush at end)
        Console.WriteLine("  [Producer Test - Batched]");
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = BootstrapServers,
            Acks = Acks.Leader,
            LingerMs = 5,
            BatchSize = 65536,
            QueueBufferingMaxMessages = 2000000,
            QueueBufferingMaxKbytes = 2097152,
        };

        double producerMsgPerSec, producerMBPerSec;
        double producerSyncMsgPerSec, producerSyncMBPerSec;
        using (var producer = new ProducerBuilder<string, byte[]>(producerConfig).Build())
        {
            var sw = Stopwatch.StartNew();
            for (int i = 0; i < messageCount; i++)
            {
                producer.Produce(topicName, new Message<string, byte[]>
                {
                    Key = i.ToString(),
                    Value = messageValue
                });

                if (i > 0 && i % 50_000 == 0)
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

        // Producer test - SYNC (per-message ack, for fair comparison with native)
        Console.WriteLine("  [Producer Test - Sync/Per-Message]");
        var syncTopicName = $"kafka-sync-{Guid.NewGuid():N}";
        var syncProducerConfig = new ProducerConfig
        {
            BootstrapServers = BootstrapServers,
            Acks = Acks.Leader,
            LingerMs = 0,  // No batching delay
            BatchSize = 1, // Minimal batch
        };
        var syncCount = Math.Min(messageCount, 10000); // Limit sync test to 10K messages

        using (var producer = new ProducerBuilder<string, byte[]>(syncProducerConfig).Build())
        {
            var sw = Stopwatch.StartNew();
            for (int i = 0; i < syncCount; i++)
            {
                await producer.ProduceAsync(syncTopicName, new Message<string, byte[]>
                {
                    Key = i.ToString(),
                    Value = messageValue
                });
            }
            sw.Stop();

            var produceMs = sw.ElapsedMilliseconds;
            producerSyncMsgPerSec = syncCount * 1000.0 / produceMs;
            producerSyncMBPerSec = (long)syncCount * messageSize / 1024.0 / 1024.0 * 1000.0 / produceMs;
            Console.WriteLine($"    Messages: {syncCount:N0} (limited for fair comparison)");
            Console.WriteLine($"    Time: {produceMs:N0} ms");
            Console.WriteLine($"    Throughput: {producerSyncMsgPerSec:N0} msg/sec ({producerSyncMBPerSec:N1} MB/sec)");
        }

        // Consumer test
        Console.WriteLine("  [Consumer Test]");
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            GroupId = $"kafka-bench-consumer-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        };

        double consumerMsgPerSec, consumerMBPerSec;
        using (var consumer = new ConsumerBuilder<string, byte[]>(consumerConfig).Build())
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

                if (consumed > 0 && consumed % 50_000 == 0)
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

        return new BenchmarkResults(producerMsgPerSec, producerMBPerSec, producerSyncMsgPerSec, producerSyncMBPerSec, consumerMsgPerSec, consumerMBPerSec);
    }

    private static async Task<BenchmarkResults> RunNativeClientBenchmarkAsync(int messageCount, byte[] messageValue)
    {
        var topicName = $"native-bench-{Guid.NewGuid():N}";
        var batchTopicName = $"native-batch-{Guid.NewGuid():N}";
        var messageSize = messageValue.Length;

        await using var client = new SurgewaveNativeClient("localhost", 9092);
        await client.ConnectAsync();

        // Create topics first
        try
        {
            await client.Topics.CreateAsync(topicName, 1);
            await client.Topics.CreateAsync(batchTopicName, 1);
            Console.WriteLine($"  Created topic: {topicName}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"  Topic creation: {ex.Message}");
        }

        // Producer test - sync (same as Kafka sync for fair comparison)
        Console.WriteLine("  [Producer Test - Sync/Per-Message]");
        var keyBytes = Encoding.UTF8.GetBytes("key");
        var syncCount = Math.Min(messageCount, 10000); // Same limit as Kafka sync test
        var sw = Stopwatch.StartNew();
        for (int i = 0; i < syncCount; i++)
        {
            await client.Messaging.SendAsync(topicName, 0, keyBytes, messageValue);
        }
        sw.Stop();

        var produceMs = sw.ElapsedMilliseconds;
        var producerMsgPerSec = syncCount * 1000.0 / produceMs;
        var producerMBPerSec = (long)syncCount * messageSize / 1024.0 / 1024.0 * 1000.0 / produceMs;
        Console.WriteLine($"    Messages: {syncCount:N0} (same as Kafka sync test)");
        Console.WriteLine($"    Time: {produceMs:N0} ms");
        Console.WriteLine($"    Throughput: {producerMsgPerSec:N0} msg/sec ({producerMBPerSec:N1} MB/sec)");

        // Producer test - Batched (using SurgewaveBatchingProducer)
        Console.WriteLine("  [Producer Test - Batched]");
        double batchedMsgPerSec, batchedMBPerSec;
        await using (var batchProducer = new SurgewaveBatchingProducer(
            client, batchTopicName, 0,
            maxBatchSize: 1000,
            maxBatchBytes: 1024 * 1024,
            lingerTime: TimeSpan.FromMilliseconds(5)))
        {
            sw.Restart();
            for (int i = 0; i < messageCount; i++)
            {
                await batchProducer.ProduceAsync(keyBytes, messageValue);

                if (i > 0 && i % 50_000 == 0)
                {
                    Console.WriteLine($"    Produced {i:N0} messages...");
                }
            }
            await batchProducer.FlushAsync();
            sw.Stop();

            var batchedMs = sw.ElapsedMilliseconds;
            batchedMsgPerSec = messageCount * 1000.0 / batchedMs;
            batchedMBPerSec = (long)messageCount * messageSize / 1024.0 / 1024.0 * 1000.0 / batchedMs;
            Console.WriteLine($"    Messages: {messageCount:N0}");
            Console.WriteLine($"    Time: {batchedMs:N0} ms");
            Console.WriteLine($"    Throughput: {batchedMsgPerSec:N0} msg/sec ({batchedMBPerSec:N1} MB/sec)");
        }

        // Consumer test
        Console.WriteLine("  [Consumer Test]");
        sw.Restart();
        var consumed = 0;
        long offset = 0;

        while (consumed < messageCount)
        {
            var result = await client.Messaging.ReceiveAsync(topicName, 0, offset, maxBytes: 1024 * 1024);
            if (result.Messages.Count == 0)
            {
                break;
            }

            foreach (var msg in result.Messages)
            {
                consumed++;
                offset = msg.Offset + 1;
            }

            if (consumed > 0 && consumed % 50_000 == 0)
            {
                Console.WriteLine($"    Consumed {consumed:N0} messages...");
            }
        }
        sw.Stop();

        var consumeMs = sw.ElapsedMilliseconds;
        var consumerMsgPerSec = consumed * 1000.0 / consumeMs;
        var consumerMBPerSec = (long)consumed * messageSize / 1024.0 / 1024.0 * 1000.0 / consumeMs;
        Console.WriteLine($"    Messages: {consumed:N0}");
        Console.WriteLine($"    Time: {consumeMs:N0} ms");
        Console.WriteLine($"    Throughput: {consumerMsgPerSec:N0} msg/sec ({consumerMBPerSec:N1} MB/sec)");

        return new BenchmarkResults(batchedMsgPerSec, batchedMBPerSec, producerMsgPerSec, producerMBPerSec, consumerMsgPerSec, consumerMBPerSec);
    }

    private sealed record BenchmarkResults(
        double ProducerBatchedMsgPerSec,
        double ProducerBatchedMBPerSec,
        double ProducerSyncMsgPerSec,
        double ProducerSyncMBPerSec,
        double ConsumerMsgPerSec,
        double ConsumerMBPerSec);
}
