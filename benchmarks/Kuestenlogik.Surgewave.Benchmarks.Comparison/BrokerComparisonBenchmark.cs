using System.Diagnostics;
using Confluent.Kafka;

namespace Kuestenlogik.Surgewave.Benchmarks.Comparison;

/// <summary>
/// Comparison benchmark between Surgewave broker and Apache Kafka broker.
/// Both use the same Kafka wire protocol client (Confluent.Kafka).
/// This measures the broker implementation performance, not protocol differences.
/// </summary>
public static class BrokerComparisonBenchmark
{
    private const int DefaultMessageCount = 100_000;
    private const int DefaultMessageSize = 1024;

    public static async Task RunAsync(string[] args)
    {
        var messageCount = args.Length > 0 ? int.Parse(args[0]) : DefaultMessageCount;
        var messageSize = args.Length > 1 ? int.Parse(args[1]) : DefaultMessageSize;
        var surgewaveBootstrap = args.Length > 2 ? args[2] : "localhost:9092";
        var kafkaBootstrap = args.Length > 3 ? args[3] : "localhost:9093";

        Console.WriteLine("╔════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║     Broker Comparison Benchmark                                ║");
        Console.WriteLine("║     Surgewave Broker vs Apache Kafka (same Kafka client)           ║");
        Console.WriteLine("╚════════════════════════════════════════════════════════════════╝");
        Console.WriteLine();
        Console.WriteLine($"Configuration:");
        Console.WriteLine($"  Messages:       {messageCount:N0}");
        Console.WriteLine($"  Message size:   {messageSize:N0} bytes");
        Console.WriteLine($"  Total data:     {(long)messageCount * messageSize / 1024 / 1024:N0} MB");
        Console.WriteLine($"  Surgewave broker:   {surgewaveBootstrap}");
        Console.WriteLine($"  Kafka broker:   {kafkaBootstrap}");
        Console.WriteLine();

        // Create message payload
        var messageValue = new byte[messageSize];
        Random.Shared.NextBytes(messageValue);

        // Test Surgewave broker
        Console.WriteLine("═══════════════════════════════════════════════════════════════════");
        Console.WriteLine("  Surgewave BROKER (Kafka wire protocol)");
        Console.WriteLine("═══════════════════════════════════════════════════════════════════");
        var surgewaveResults = await RunBrokerBenchmarkAsync(surgewaveBootstrap, "surgewave", messageCount, messageValue);

        // Test Kafka broker
        Console.WriteLine();
        Console.WriteLine("═══════════════════════════════════════════════════════════════════");
        Console.WriteLine("  APACHE KAFKA BROKER");
        Console.WriteLine("═══════════════════════════════════════════════════════════════════");
        BenchmarkResults? kafkaResults = null;
        try
        {
            kafkaResults = await RunBrokerBenchmarkAsync(kafkaBootstrap, "kafka", messageCount, messageValue);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"  ERROR: Could not connect to Kafka broker at {kafkaBootstrap}");
            Console.WriteLine($"  {ex.Message}");
            Console.WriteLine();
            Console.WriteLine("  To run Kafka for comparison, use Docker:");
            Console.WriteLine("    docker run -d --name kafka -p 9093:9093 \\");
            Console.WriteLine("      -e KAFKA_CFG_NODE_ID=0 \\");
            Console.WriteLine("      -e KAFKA_CFG_PROCESS_ROLES=controller,broker \\");
            Console.WriteLine("      -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9093,CONTROLLER://:9094 \\");
            Console.WriteLine("      -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9093 \\");
            Console.WriteLine("      -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=0@localhost:9094 \\");
            Console.WriteLine("      -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \\");
            Console.WriteLine("      bitnami/kafka:latest");
            Console.WriteLine();
        }

        // Print comparison
        if (kafkaResults != null)
        {
            PrintComparison(surgewaveResults, kafkaResults);
        }
        else
        {
            Console.WriteLine("═══════════════════════════════════════════════════════════════════");
            Console.WriteLine("  Surgewave BROKER RESULTS (Kafka broker unavailable for comparison)");
            Console.WriteLine("═══════════════════════════════════════════════════════════════════");
            Console.WriteLine();
            Console.WriteLine($"  Producer (batched):  {surgewaveResults.ProducerBatchedMsgPerSec:N0} msg/sec ({surgewaveResults.ProducerBatchedMBPerSec:N1} MB/sec)");
            Console.WriteLine($"  Producer (sync):     {surgewaveResults.ProducerSyncMsgPerSec:N0} msg/sec ({surgewaveResults.ProducerSyncMBPerSec:N1} MB/sec)");
            Console.WriteLine($"  Consumer:            {surgewaveResults.ConsumerMsgPerSec:N0} msg/sec ({surgewaveResults.ConsumerMBPerSec:N1} MB/sec)");
            Console.WriteLine();
        }
    }

    private static void PrintComparison(BenchmarkResults surgewave, BenchmarkResults kafka)
    {
        Console.WriteLine();
        Console.WriteLine("═══════════════════════════════════════════════════════════════════");
        Console.WriteLine("  COMPARISON SUMMARY");
        Console.WriteLine("═══════════════════════════════════════════════════════════════════");
        Console.WriteLine();
        Console.WriteLine($"{"Metric",-30} {"Surgewave Broker",-18} {"Apache Kafka",-18} {"Delta",-15}");
        Console.WriteLine(new string('-', 85));

        PrintComparisonRow("Batched Producer (msg/sec)", surgewave.ProducerBatchedMsgPerSec, kafka.ProducerBatchedMsgPerSec);
        PrintComparisonRow("Batched Producer (MB/sec)", surgewave.ProducerBatchedMBPerSec, kafka.ProducerBatchedMBPerSec);
        PrintComparisonRow("Sync Producer (msg/sec)", surgewave.ProducerSyncMsgPerSec, kafka.ProducerSyncMsgPerSec);
        PrintComparisonRow("Sync Producer (MB/sec)", surgewave.ProducerSyncMBPerSec, kafka.ProducerSyncMBPerSec);
        PrintComparisonRow("Consumer (msg/sec)", surgewave.ConsumerMsgPerSec, kafka.ConsumerMsgPerSec);
        PrintComparisonRow("Consumer (MB/sec)", surgewave.ConsumerMBPerSec, kafka.ConsumerMBPerSec);

        Console.WriteLine();

        // Calculate overall score
        var surgewaveScore = surgewave.ProducerBatchedMsgPerSec + surgewave.ConsumerMsgPerSec;
        var kafkaScore = kafka.ProducerBatchedMsgPerSec + kafka.ConsumerMsgPerSec;
        var overallDelta = (surgewaveScore - kafkaScore) / kafkaScore * 100;

        Console.WriteLine($"Overall throughput score: Surgewave {(overallDelta >= 0 ? "+" : "")}{overallDelta:N1}% vs Kafka");
        Console.WriteLine();
    }

    private static void PrintComparisonRow(string metric, double surgewave, double kafka)
    {
        if (kafka == 0)
        {
            Console.WriteLine($"{metric,-30} {surgewave,16:N0} {"N/A",16} {"N/A",-12}");
            return;
        }
        var delta = (surgewave - kafka) / kafka * 100;
        var deltaStr = delta >= 0 ? $"+{delta:N1}%" : $"{delta:N1}%";
        var winner = surgewave > kafka ? "Surgewave" : "Kafka";
        Console.WriteLine($"{metric,-30} {surgewave,16:N0} {kafka,16:N0} {deltaStr,-12} ({winner})");
    }

    private static async Task<BenchmarkResults> RunBrokerBenchmarkAsync(
        string bootstrapServers,
        string brokerName,
        int messageCount,
        byte[] messageValue)
    {
        var topicName = $"{brokerName}-bench-{Guid.NewGuid():N}";
        var messageSize = messageValue.Length;

        // Test connection first
        Console.WriteLine($"  Connecting to {bootstrapServers}...");
        var adminConfig = new AdminClientConfig { BootstrapServers = bootstrapServers };
        using (var admin = new AdminClientBuilder(adminConfig).Build())
        {
            var metadata = admin.GetMetadata(TimeSpan.FromSeconds(10));
            Console.WriteLine($"  Connected! Broker: {metadata.Brokers[0].Host}:{metadata.Brokers[0].Port}");
        }

        // Producer test - BATCHED
        Console.WriteLine("  [Producer Test - Batched]");
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            Acks = Acks.Leader,
            LingerMs = 5,
            BatchSize = 65536,
            QueueBufferingMaxMessages = 2000000,
            QueueBufferingMaxKbytes = 2097152,
        };

        double producerBatchedMsgPerSec, producerBatchedMBPerSec;
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

            var produceMs = Math.Max(1, sw.ElapsedMilliseconds);
            producerBatchedMsgPerSec = messageCount * 1000.0 / produceMs;
            producerBatchedMBPerSec = (long)messageCount * messageSize / 1024.0 / 1024.0 * 1000.0 / produceMs;
            Console.WriteLine($"    Time: {produceMs:N0} ms");
            Console.WriteLine($"    Throughput: {producerBatchedMsgPerSec:N0} msg/sec ({producerBatchedMBPerSec:N1} MB/sec)");
        }

        // Producer test - SYNC
        Console.WriteLine("  [Producer Test - Sync/Per-Message]");
        var syncTopicName = $"{brokerName}-sync-{Guid.NewGuid():N}";
        var syncProducerConfig = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            Acks = Acks.Leader,
            LingerMs = 0,
            BatchSize = 1,
        };
        var syncCount = Math.Min(messageCount, 5000); // Limit sync test

        double producerSyncMsgPerSec, producerSyncMBPerSec;
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

            var produceMs = Math.Max(1, sw.ElapsedMilliseconds);
            producerSyncMsgPerSec = syncCount * 1000.0 / produceMs;
            producerSyncMBPerSec = (long)syncCount * messageSize / 1024.0 / 1024.0 * 1000.0 / produceMs;
            Console.WriteLine($"    Messages: {syncCount:N0}");
            Console.WriteLine($"    Time: {produceMs:N0} ms");
            Console.WriteLine($"    Throughput: {producerSyncMsgPerSec:N0} msg/sec ({producerSyncMBPerSec:N1} MB/sec)");
        }

        // Consumer test
        Console.WriteLine("  [Consumer Test]");
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = $"{brokerName}-bench-consumer-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            FetchMinBytes = 1,
            FetchMaxBytes = 52428800, // 50MB
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

            var consumeMs = Math.Max(1, sw.ElapsedMilliseconds);
            consumerMsgPerSec = consumed * 1000.0 / consumeMs;
            consumerMBPerSec = (long)consumed * messageSize / 1024.0 / 1024.0 * 1000.0 / consumeMs;
            Console.WriteLine($"    Messages: {consumed:N0}");
            Console.WriteLine($"    Time: {consumeMs:N0} ms");
            Console.WriteLine($"    Throughput: {consumerMsgPerSec:N0} msg/sec ({consumerMBPerSec:N1} MB/sec)");
        }

        return new BenchmarkResults(
            producerBatchedMsgPerSec, producerBatchedMBPerSec,
            producerSyncMsgPerSec, producerSyncMBPerSec,
            consumerMsgPerSec, consumerMBPerSec);
    }

    private sealed record BenchmarkResults(
        double ProducerBatchedMsgPerSec,
        double ProducerBatchedMBPerSec,
        double ProducerSyncMsgPerSec,
        double ProducerSyncMBPerSec,
        double ConsumerMsgPerSec,
        double ConsumerMBPerSec);
}
