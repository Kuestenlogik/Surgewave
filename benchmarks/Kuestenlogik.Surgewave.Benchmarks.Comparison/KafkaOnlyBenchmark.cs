using System.Diagnostics;
using Confluent.Kafka;

namespace Kuestenlogik.Surgewave.Benchmarks.Comparison;

/// <summary>
/// Benchmark for Kafka client only (Confluent.Kafka) against real Kafka broker.
/// This gives us the pure Kafka baseline numbers.
/// </summary>
public static class KafkaOnlyBenchmark
{
    private const string BootstrapServers = "localhost:9092";
    private const int DefaultMessageCount = 100_000;
    private const int DefaultMessageSize = 1024;

    public static async Task RunAsync(string[] args)
    {
        var messageCount = args.Length > 0 ? int.Parse(args[0]) : DefaultMessageCount;
        var messageSize = args.Length > 1 ? int.Parse(args[1]) : DefaultMessageSize;

        Console.WriteLine("╔════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║     KAFKA-ONLY BENCHMARK (Confluent.Kafka vs Real Kafka)       ║");
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

        var topicName = $"kafka-only-bench-{Guid.NewGuid():N}";

        // Producer test - BATCHED (fire-and-forget with flush at end)
        Console.WriteLine("═══════════════════════════════════════════════════════════════════");
        Console.WriteLine("  PRODUCER TEST - BATCHED (fire-and-forget + flush)");
        Console.WriteLine("═══════════════════════════════════════════════════════════════════");
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

        // Producer test - SYNC (per-message ack)
        Console.WriteLine();
        Console.WriteLine("═══════════════════════════════════════════════════════════════════");
        Console.WriteLine("  PRODUCER TEST - SYNC (per-message acknowledgment)");
        Console.WriteLine("═══════════════════════════════════════════════════════════════════");
        var syncTopicName = $"kafka-sync-{Guid.NewGuid():N}";
        var syncProducerConfig = new ProducerConfig
        {
            BootstrapServers = BootstrapServers,
            Acks = Acks.Leader,
            LingerMs = 0,
            BatchSize = 1,
        };
        var syncCount = Math.Min(messageCount, 10000);

        double syncMsgPerSec, syncMBPerSec;
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
            syncMsgPerSec = syncCount * 1000.0 / produceMs;
            syncMBPerSec = (long)syncCount * messageSize / 1024.0 / 1024.0 * 1000.0 / produceMs;
            Console.WriteLine($"    Messages: {syncCount:N0}");
            Console.WriteLine($"    Time: {produceMs:N0} ms");
            Console.WriteLine($"    Throughput: {syncMsgPerSec:N0} msg/sec ({syncMBPerSec:N1} MB/sec)");
        }

        // Consumer test
        Console.WriteLine();
        Console.WriteLine("═══════════════════════════════════════════════════════════════════");
        Console.WriteLine("  CONSUMER TEST");
        Console.WriteLine("═══════════════════════════════════════════════════════════════════");
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

        // Summary
        Console.WriteLine();
        Console.WriteLine("═══════════════════════════════════════════════════════════════════");
        Console.WriteLine("  KAFKA-ONLY BASELINE SUMMARY");
        Console.WriteLine("═══════════════════════════════════════════════════════════════════");
        Console.WriteLine($"  Batched Producer: {producerMsgPerSec:N0} msg/sec ({producerMBPerSec:N1} MB/sec)");
        Console.WriteLine($"  Sync Producer:    {syncMsgPerSec:N0} msg/sec ({syncMBPerSec:N1} MB/sec)");
        Console.WriteLine($"  Consumer:         {consumerMsgPerSec:N0} msg/sec ({consumerMBPerSec:N1} MB/sec)");
        Console.WriteLine();
    }
}
