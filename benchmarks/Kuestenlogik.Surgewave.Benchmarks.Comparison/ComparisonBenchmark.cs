using System.Diagnostics;
using Confluent.Kafka;
using Kuestenlogik.Surgewave.Benchmarks.Comparison.Infrastructure;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Client.Native.Operations.Performance;
using Kuestenlogik.Surgewave.Runtime;

namespace Kuestenlogik.Surgewave.Benchmarks.Comparison;

/// <summary>
/// Comprehensive broker/protocol comparison benchmark with automatic container management:
/// 1. Pure Kafka (Apache Kafka via Testcontainers + Confluent.Kafka client)
/// 2. Redpanda (Redpanda via Testcontainers + Confluent.Kafka client)
/// 3. Surgewave + Kafka Protocol (Surgewave broker + Confluent.Kafka client)
/// 4. Surgewave Native (Surgewave broker + SurgewaveNativeClient)
///
/// Uses Testcontainers.NET to automatically start Kafka and Redpanda containers.
///
/// Run with: dotnet run -- compare [msgCount] [msgSize] [batchSize] [--skip-kafka] [--skip-redpanda]
/// </summary>
public static class ComparisonBenchmark
{
    public static async Task RunAsync(string[] args)
    {
        var messageCount = 100_000;
        var messageSizeBytes = 100;
        var batchSize = 1000;
        var skipKafka = false;
        var skipRedpanda = false;

        // Parse arguments
        for (int i = 0; i < args.Length; i++)
        {
            if (args[i].Equals("--skip-kafka", StringComparison.OrdinalIgnoreCase))
                skipKafka = true;
            else if (args[i].Equals("--skip-redpanda", StringComparison.OrdinalIgnoreCase))
                skipRedpanda = true;
            else if (int.TryParse(args[i], out var val))
            {
                if (messageCount == 100_000 && i == 0)
                    messageCount = val;
                else if (messageSizeBytes == 100 && i == 1)
                    messageSizeBytes = val;
                else if (batchSize == 1000 && i == 2)
                    batchSize = val;
            }
        }

        Console.WriteLine("╔══════════════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║       FOUR-WAY BROKER/PROTOCOL COMPARISON (AUTO CONTAINERS)              ║");
        Console.WriteLine("╠══════════════════════════════════════════════════════════════════════════╣");
        Console.WriteLine("║  1. Pure Kafka      = Apache Kafka (Testcontainers) + Confluent.Kafka    ║");
        Console.WriteLine("║  2. Redpanda        = Redpanda (Testcontainers) + Confluent.Kafka        ║");
        Console.WriteLine("║  3. Surgewave + Kafka   = Surgewave broker + Confluent.Kafka client (drop-in)    ║");
        Console.WriteLine("║  4. Surgewave Native    = Surgewave broker + SurgewaveNativeClient (optimized)       ║");
        Console.WriteLine("╚══════════════════════════════════════════════════════════════════════════╝");
        Console.WriteLine();
        Console.WriteLine($"Configuration:");
        Console.WriteLine($"  Messages:         {messageCount:N0}");
        Console.WriteLine($"  Message size:     {messageSizeBytes} bytes");
        Console.WriteLine($"  Batch size:       {batchSize}");
        Console.WriteLine($"  Total data:       {(long)messageCount * messageSizeBytes / 1024 / 1024:N1} MB");
        Console.WriteLine($"  Skip Kafka:       {skipKafka}");
        Console.WriteLine($"  Skip Redpanda:    {skipRedpanda}");
        Console.WriteLine();

        // Prepare test data
        var messageValue = new byte[messageSizeBytes];
        Random.Shared.NextBytes(messageValue);

        // Results storage
        BenchmarkResult? kafkaResult = null;
        BenchmarkResult? redpandaResult = null;
        BenchmarkResult? surgewaveKafkaResult = null;
        BenchmarkResult? surgewaveNativeResult = null;

        try
        {
            // 1. Test Pure Kafka (if not skipped)
            if (!skipKafka)
            {
                Console.WriteLine("═══════════════════════════════════════════════════════════════════════════");
                Console.WriteLine("  [1/4] PURE KAFKA (Apache Kafka via Testcontainers)");
                Console.WriteLine("═══════════════════════════════════════════════════════════════════════════");
                try
                {
                    var kafkaBootstrap = await ContainerManager.GetKafkaBootstrapServersAsync();
                    kafkaResult = await RunKafkaBenchmarkAsync(kafkaBootstrap, messageCount, messageSizeBytes, messageValue, "kafka", batchSize);
                    Console.WriteLine($"  Producer: {kafkaResult.ProducerMsgPerSec:N0} msg/sec ({kafkaResult.ProducerMBPerSec:N1} MB/sec)");
                    Console.WriteLine($"  Consumer: {kafkaResult.ConsumerMsgPerSec:N0} msg/sec ({kafkaResult.ConsumerMBPerSec:N1} MB/sec)");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"  SKIPPED: Could not start Kafka container");
                    Console.WriteLine($"  Error: {ex.Message}");
                }
            }
            else
            {
                Console.WriteLine();
                Console.WriteLine("  [1/4] PURE KAFKA - SKIPPED (--skip-kafka)");
            }

            // 2. Test Redpanda (if not skipped)
            Console.WriteLine();
            if (!skipRedpanda)
            {
                Console.WriteLine("═══════════════════════════════════════════════════════════════════════════");
                Console.WriteLine("  [2/4] REDPANDA (Redpanda via Testcontainers)");
                Console.WriteLine("═══════════════════════════════════════════════════════════════════════════");
                try
                {
                    var redpandaBootstrap = await ContainerManager.GetRedpandaBootstrapServersAsync();
                    redpandaResult = await RunKafkaBenchmarkAsync(redpandaBootstrap, messageCount, messageSizeBytes, messageValue, "redpanda", batchSize);
                    Console.WriteLine($"  Producer: {redpandaResult.ProducerMsgPerSec:N0} msg/sec ({redpandaResult.ProducerMBPerSec:N1} MB/sec)");
                    Console.WriteLine($"  Consumer: {redpandaResult.ConsumerMsgPerSec:N0} msg/sec ({redpandaResult.ConsumerMBPerSec:N1} MB/sec)");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"  SKIPPED: Could not start Redpanda container");
                    Console.WriteLine($"  Error: {ex.Message}");
                }
            }
            else
            {
                Console.WriteLine("  [2/4] REDPANDA - SKIPPED (--skip-redpanda)");
            }

            // 3. Start Embedded Surgewave for remaining tests
            Console.WriteLine();
            Console.WriteLine("Starting embedded Surgewave broker...");
            await using var surgewave = await SurgewaveRuntime.CreateBuilder()
                .WithPort(0)
                .WithAutoCreateTopics(true)
                .WithPartitions(1)
                .Build()
                .StartAsync();
            Console.WriteLine($"Embedded Surgewave started on port {surgewave.Port}");

            // 4. Test Surgewave + Kafka Protocol
            Console.WriteLine();
            Console.WriteLine("═══════════════════════════════════════════════════════════════════════════");
            Console.WriteLine("  [3/4] Surgewave + KAFKA PROTOCOL (Surgewave broker + Confluent.Kafka client)");
            Console.WriteLine("═══════════════════════════════════════════════════════════════════════════");
            surgewaveKafkaResult = await RunKafkaBenchmarkAsync(surgewave.BootstrapServers, messageCount, messageSizeBytes, messageValue, "surgewave-kafka", batchSize);
            Console.WriteLine($"  Producer: {surgewaveKafkaResult.ProducerMsgPerSec:N0} msg/sec ({surgewaveKafkaResult.ProducerMBPerSec:N1} MB/sec)");
            Console.WriteLine($"  Consumer: {surgewaveKafkaResult.ConsumerMsgPerSec:N0} msg/sec ({surgewaveKafkaResult.ConsumerMBPerSec:N1} MB/sec)");

            // 5. Test Surgewave Native Protocol
            Console.WriteLine();
            Console.WriteLine("═══════════════════════════════════════════════════════════════════════════");
            Console.WriteLine("  [4/4] Surgewave NATIVE (Surgewave broker + SurgewaveNativeClient)");
            Console.WriteLine("═══════════════════════════════════════════════════════════════════════════");
            surgewaveNativeResult = await RunNativeBenchmarkAsync(surgewave.Host, surgewave.Port, messageCount, messageSizeBytes, messageValue, batchSize);
            Console.WriteLine($"  Producer: {surgewaveNativeResult.ProducerMsgPerSec:N0} msg/sec ({surgewaveNativeResult.ProducerMBPerSec:N1} MB/sec)");
            Console.WriteLine($"  Consumer: {surgewaveNativeResult.ConsumerMsgPerSec:N0} msg/sec ({surgewaveNativeResult.ConsumerMBPerSec:N1} MB/sec)");

            // Print comparison table
            PrintComparisonSummary(kafkaResult, redpandaResult, surgewaveKafkaResult, surgewaveNativeResult);
        }
        finally
        {
            // Cleanup containers
            Console.WriteLine();
            Console.WriteLine("Cleaning up containers...");
            await ContainerManager.StopAllAsync();
            Console.WriteLine("Done!");
        }
    }

    private static void PrintComparisonSummary(
        BenchmarkResult? kafkaResult,
        BenchmarkResult? redpandaResult,
        BenchmarkResult surgewaveKafkaResult,
        BenchmarkResult surgewaveNativeResult)
    {
        Console.WriteLine();
        Console.WriteLine("╔══════════════════════════════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║                              COMPARISON SUMMARY                                          ║");
        Console.WriteLine("╚══════════════════════════════════════════════════════════════════════════════════════════╝");
        Console.WriteLine();

        // Header
        Console.WriteLine($"{"Metric",-28} {"Pure Kafka",-14} {"Redpanda",-14} {"Surgewave+Kafka",-14} {"Surgewave Native",-14}");
        Console.WriteLine(new string('─', 90));

        // Producer comparison
        var kafkaProd = kafkaResult?.ProducerMsgPerSec ?? 0;
        var redpandaProd = redpandaResult?.ProducerMsgPerSec ?? 0;
        var surgewaveKafkaProd = surgewaveKafkaResult.ProducerMsgPerSec;
        var surgewaveNativeProd = surgewaveNativeResult.ProducerMsgPerSec;

        Console.Write($"{"Producer (msg/sec)",-28}");
        Console.Write($"{(kafkaResult != null ? kafkaProd.ToString("N0") : "N/A"),-14}");
        Console.Write($"{(redpandaResult != null ? redpandaProd.ToString("N0") : "N/A"),-14}");
        Console.Write($"{surgewaveKafkaProd,-14:N0}");
        Console.WriteLine($"{surgewaveNativeProd,-14:N0}");

        Console.Write($"{"Producer (MB/sec)",-28}");
        Console.Write($"{(kafkaResult != null ? kafkaResult.ProducerMBPerSec.ToString("N1") : "N/A"),-14}");
        Console.Write($"{(redpandaResult != null ? redpandaResult.ProducerMBPerSec.ToString("N1") : "N/A"),-14}");
        Console.Write($"{surgewaveKafkaResult.ProducerMBPerSec,-14:N1}");
        Console.WriteLine($"{surgewaveNativeResult.ProducerMBPerSec,-14:N1}");

        // Consumer comparison
        var kafkaCons = kafkaResult?.ConsumerMsgPerSec ?? 0;
        var redpandaCons = redpandaResult?.ConsumerMsgPerSec ?? 0;
        var surgewaveKafkaCons = surgewaveKafkaResult.ConsumerMsgPerSec;
        var surgewaveNativeCons = surgewaveNativeResult.ConsumerMsgPerSec;

        Console.Write($"{"Consumer (msg/sec)",-28}");
        Console.Write($"{(kafkaResult != null ? kafkaCons.ToString("N0") : "N/A"),-14}");
        Console.Write($"{(redpandaResult != null ? redpandaCons.ToString("N0") : "N/A"),-14}");
        Console.Write($"{surgewaveKafkaCons,-14:N0}");
        Console.WriteLine($"{surgewaveNativeCons,-14:N0}");

        Console.Write($"{"Consumer (MB/sec)",-28}");
        Console.Write($"{(kafkaResult != null ? kafkaResult.ConsumerMBPerSec.ToString("N1") : "N/A"),-14}");
        Console.Write($"{(redpandaResult != null ? redpandaResult.ConsumerMBPerSec.ToString("N1") : "N/A"),-14}");
        Console.Write($"{surgewaveKafkaResult.ConsumerMBPerSec,-14:N1}");
        Console.WriteLine($"{surgewaveNativeResult.ConsumerMBPerSec,-14:N1}");

        // Relative performance vs Pure Kafka
        Console.WriteLine();
        Console.WriteLine("Relative Performance (vs Pure Kafka baseline):");
        Console.WriteLine(new string('─', 90));

        if (kafkaResult != null)
        {
            if (redpandaResult != null)
            {
                var redpandaProdDelta = (redpandaProd - kafkaProd) / kafkaProd * 100;
                var redpandaConsDelta = (redpandaCons - kafkaCons) / kafkaCons * 100;
                Console.WriteLine($"  Redpanda Producer:      {(redpandaProdDelta >= 0 ? "+" : "")}{redpandaProdDelta:N1}% vs Kafka");
                Console.WriteLine($"  Redpanda Consumer:      {(redpandaConsDelta >= 0 ? "+" : "")}{redpandaConsDelta:N1}% vs Kafka");
            }

            var surgewaveKafkaProdDelta = (surgewaveKafkaProd - kafkaProd) / kafkaProd * 100;
            var surgewaveNativeProdDelta = (surgewaveNativeProd - kafkaProd) / kafkaProd * 100;
            var surgewaveKafkaConsDelta = (surgewaveKafkaCons - kafkaCons) / kafkaCons * 100;
            var surgewaveNativeConsDelta = (surgewaveNativeCons - kafkaCons) / kafkaCons * 100;

            Console.WriteLine($"  Surgewave+Kafka Producer:   {(surgewaveKafkaProdDelta >= 0 ? "+" : "")}{surgewaveKafkaProdDelta:N1}% vs Kafka");
            Console.WriteLine($"  Surgewave Native Producer:  {(surgewaveNativeProdDelta >= 0 ? "+" : "")}{surgewaveNativeProdDelta:N1}% vs Kafka");
            Console.WriteLine($"  Surgewave+Kafka Consumer:   {(surgewaveKafkaConsDelta >= 0 ? "+" : "")}{surgewaveKafkaConsDelta:N1}% vs Kafka");
            Console.WriteLine($"  Surgewave Native Consumer:  {(surgewaveNativeConsDelta >= 0 ? "+" : "")}{surgewaveNativeConsDelta:N1}% vs Kafka");
        }
        else
        {
            Console.WriteLine("  (Kafka baseline not available - comparisons skipped)");
        }

        // Native vs Kafka Protocol (Surgewave broker)
        Console.WriteLine();
        Console.WriteLine("Surgewave Protocol Advantage (Native vs Kafka protocol on same broker):");
        Console.WriteLine(new string('─', 90));
        var nativeVsKafkaProd = (surgewaveNativeProd - surgewaveKafkaProd) / surgewaveKafkaProd * 100;
        var nativeVsKafkaCons = (surgewaveNativeCons - surgewaveKafkaCons) / surgewaveKafkaCons * 100;
        Console.WriteLine($"  Producer: {(nativeVsKafkaProd >= 0 ? "+" : "")}{nativeVsKafkaProd:N1}% (Native over Kafka protocol)");
        Console.WriteLine($"  Consumer: {(nativeVsKafkaCons >= 0 ? "+" : "")}{nativeVsKafkaCons:N1}% (Native over Kafka protocol)");

        Console.WriteLine();
        Console.WriteLine("Note: Surgewave Native protocol is optimized for Surgewave, while Kafka protocol");
        Console.WriteLine("      provides drop-in compatibility with existing Kafka clients.");
    }

    private static async Task<BenchmarkResult> RunKafkaBenchmarkAsync(
        string bootstrapServers, int messageCount, int messageSize, byte[] messageValue, string prefix, int batchSize)
    {
        var topicName = $"{prefix}-bench-{Guid.NewGuid():N}";

        // Producer test
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
            }
            producer.Flush(TimeSpan.FromSeconds(60));
            sw.Stop();

            var ms = sw.ElapsedMilliseconds;
            producerMsgPerSec = messageCount * 1000.0 / ms;
            producerMBPerSec = (long)messageCount * messageSize / 1024.0 / 1024.0 * 1000.0 / ms;
        }

        // Consumer test
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = $"bench-consumer-{Guid.NewGuid():N}",
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

            while (consumed < messageCount && noMessageCount < 3)
            {
                var result = consumer.Consume(TimeSpan.FromSeconds(5));
                if (result == null)
                {
                    noMessageCount++;
                    continue;
                }
                noMessageCount = 0;
                consumed++;
            }
            sw.Stop();

            var ms = sw.ElapsedMilliseconds;
            consumerMsgPerSec = consumed * 1000.0 / ms;
            consumerMBPerSec = (long)consumed * messageSize / 1024.0 / 1024.0 * 1000.0 / ms;
        }

        return new BenchmarkResult(producerMsgPerSec, producerMBPerSec, consumerMsgPerSec, consumerMBPerSec);
    }

    private static async Task<BenchmarkResult> RunNativeBenchmarkAsync(
        string host, int port, int messageCount, int messageSize, byte[] messageValue, int batchSize)
    {
        var topicName = $"native-bench-{Guid.NewGuid():N}";

        await using var client = new SurgewaveNativeClient(host, port);
        await client.ConnectAsync();
        await client.Topics.CreateAsync(topicName, 1);

        // Producer test with batching
        await using var producer = new SurgewaveBatchingProducer(
            client, topicName, 0,
            maxBatchSize: batchSize,
            lingerTime: TimeSpan.FromMilliseconds(5));

        var sw = Stopwatch.StartNew();
        for (int i = 0; i < messageCount; i++)
        {
            await producer.ProduceAsync(null, messageValue);
        }
        await producer.FlushAsync();
        sw.Stop();

        var produceMs = sw.ElapsedMilliseconds;
        var producerMsgPerSec = messageCount * 1000.0 / produceMs;
        var producerMBPerSec = (long)messageCount * messageSize / 1024.0 / 1024.0 * 1000.0 / produceMs;

        // Consumer test
        sw.Restart();
        var consumed = 0;
        long offset = 0;

        while (consumed < messageCount)
        {
            var result = await client.Messaging.ReceiveAsync(topicName, 0, offset, 1024 * 1024);
            if (result.Messages.Count == 0)
            {
                await Task.Delay(10);
                continue;
            }
            consumed += result.Messages.Count;
            offset = result.Messages[^1].Offset + 1;
        }
        sw.Stop();

        var consumeMs = sw.ElapsedMilliseconds;
        var consumerMsgPerSec = consumed * 1000.0 / consumeMs;
        var consumerMBPerSec = (long)consumed * messageSize / 1024.0 / 1024.0 * 1000.0 / consumeMs;

        return new BenchmarkResult(producerMsgPerSec, producerMBPerSec, consumerMsgPerSec, consumerMBPerSec);
    }

    private sealed record BenchmarkResult(
        double ProducerMsgPerSec,
        double ProducerMBPerSec,
        double ConsumerMsgPerSec,
        double ConsumerMBPerSec);
}
