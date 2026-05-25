using Kuestenlogik.Surgewave.Runtime;

namespace Kuestenlogik.Surgewave.Benchmarks.Comparison;

/// <summary>
/// Competitor-first Kafka benchmark.
///
/// Default: benchmarks only Kafka and saves the result to
///   <c>artifacts/benchmarks/results/kafka.json</c>.
///
/// With <c>--include-surgewave</c>: also benchmarks embedded Surgewave (both Kafka-protocol
/// and Native-protocol variants) and saves those results separately.
///
/// Run:
///   dotnet run -- benchmark-kafka [msgCount] [msgSize] [bootstrap] [--include-surgewave]
/// </summary>
public static class KafkaBenchmark
{
    public static async Task RunAsync(string[] args)
    {
        var messageCount   = args.Length > 0 && int.TryParse(args[0], out var mc) ? mc : 100_000;
        var messageSizeBytes = args.Length > 1 && int.TryParse(args[1], out var ms) ? ms : 100;
        var kafkaBootstrap = args.Length > 2 && !args[2].StartsWith('-') ? args[2] : "localhost:29092";
        var includeSurgewave   = args.Any(a => a.Equals("--include-surgewave", StringComparison.OrdinalIgnoreCase));

        Console.WriteLine("╔══════════════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║                      KAFKA BENCHMARK                                     ║");
        Console.WriteLine("╠══════════════════════════════════════════════════════════════════════════╣");
        Console.WriteLine("║  Apache Kafka broker + Confluent.Kafka client                            ║");
        if (includeSurgewave)
            Console.WriteLine("║  + Surgewave embedded (Kafka protocol + Native protocol)                     ║");
        Console.WriteLine("╚══════════════════════════════════════════════════════════════════════════╝");
        Console.WriteLine();
        Console.WriteLine($"  Messages:      {messageCount:N0}");
        Console.WriteLine($"  Message size:  {messageSizeBytes} bytes");
        Console.WriteLine($"  Total data:    {(long)messageCount * messageSizeBytes / 1024 / 1024:N1} MB");
        Console.WriteLine($"  Kafka broker:  {kafkaBootstrap}");
        Console.WriteLine();

        var messageValue = new byte[messageSizeBytes];
        Random.Shared.NextBytes(messageValue);

        // ── Kafka ──────────────────────────────────────────────────────────
        Console.WriteLine("── Kafka (Apache Kafka + Confluent.Kafka) ──────────────────────────────────");
        try
        {
            var result = await ComparisonHelper.RunKafkaBenchmarkAsync(
                kafkaBootstrap, messageCount, messageSizeBytes, messageValue, "kafka");

            Console.WriteLine($"  Producer: {result.ProducerMsgPerSec:N0} msg/sec  ({result.ProducerMBPerSec:N1} MB/sec)");
            Console.WriteLine($"  Consumer: {result.ConsumerMsgPerSec:N0} msg/sec  ({result.ConsumerMBPerSec:N1} MB/sec)");

            BenchmarkResultStore.Save(new BenchmarkResultEntry(
                Platform:         "kafka",
                Timestamp:        DateTime.UtcNow,
                MessageCount:     messageCount,
                MessageSize:      messageSizeBytes,
                ProducerMsgPerSec: result.ProducerMsgPerSec,
                ProducerMBPerSec:  result.ProducerMBPerSec,
                ConsumerMsgPerSec: result.ConsumerMsgPerSec,
                ConsumerMBPerSec:  result.ConsumerMBPerSec));
        }
        catch (Exception ex)
        {
            Console.WriteLine($"  SKIPPED: Kafka broker not available at {kafkaBootstrap}");
            Console.WriteLine($"  Error:   {ex.Message}");
            Console.WriteLine($"  Tip:     docker run -d --name kafka -p 29092:9092 confluentinc/cp-kafka:latest ...");
        }

        if (!includeSurgewave)
            return;

        // ── Embedded Surgewave ─────────────────────────────────────────────────
        Console.WriteLine();
        Console.WriteLine("Starting embedded Surgewave broker...");
        await using var surgewave = await SurgewaveRuntime.CreateBuilder()
            .WithPort(0)
            .WithAutoCreateTopics(true)
            .WithPartitions(1)
            .Build()
            .StartAsync();
        Console.WriteLine($"  Surgewave started on port {surgewave.Port}");

        // Surgewave + Kafka Protocol
        Console.WriteLine();
        Console.WriteLine("── Surgewave + Kafka Protocol (Surgewave broker + Confluent.Kafka) ─────────────────");
        var surgewaveKafkaResult = await ComparisonHelper.RunKafkaBenchmarkAsync(
            surgewave.BootstrapServers, messageCount, messageSizeBytes, messageValue, "surgewave-kafka");
        Console.WriteLine($"  Producer: {surgewaveKafkaResult.ProducerMsgPerSec:N0} msg/sec  ({surgewaveKafkaResult.ProducerMBPerSec:N1} MB/sec)");
        Console.WriteLine($"  Consumer: {surgewaveKafkaResult.ConsumerMsgPerSec:N0} msg/sec  ({surgewaveKafkaResult.ConsumerMBPerSec:N1} MB/sec)");
        BenchmarkResultStore.Save(new BenchmarkResultEntry(
            Platform:         "surgewave-kafka",
            Timestamp:        DateTime.UtcNow,
            MessageCount:     messageCount,
            MessageSize:      messageSizeBytes,
            ProducerMsgPerSec: surgewaveKafkaResult.ProducerMsgPerSec,
            ProducerMBPerSec:  surgewaveKafkaResult.ProducerMBPerSec,
            ConsumerMsgPerSec: surgewaveKafkaResult.ConsumerMsgPerSec,
            ConsumerMBPerSec:  surgewaveKafkaResult.ConsumerMBPerSec));

        // Surgewave Native Protocol
        Console.WriteLine();
        Console.WriteLine("── Surgewave Native (Surgewave broker + SurgewaveNativeClient) ──────────────────────────");
        var surgewaveNativeResult = await ComparisonHelper.RunNativeBenchmarkAsync(
            surgewave.Host, surgewave.Port, messageCount, messageSizeBytes, messageValue);
        Console.WriteLine($"  Producer: {surgewaveNativeResult.ProducerMsgPerSec:N0} msg/sec  ({surgewaveNativeResult.ProducerMBPerSec:N1} MB/sec)");
        Console.WriteLine($"  Consumer: {surgewaveNativeResult.ConsumerMsgPerSec:N0} msg/sec  ({surgewaveNativeResult.ConsumerMBPerSec:N1} MB/sec)");
        BenchmarkResultStore.Save(new BenchmarkResultEntry(
            Platform:         "surgewave-native",
            Timestamp:        DateTime.UtcNow,
            MessageCount:     messageCount,
            MessageSize:      messageSizeBytes,
            ProducerMsgPerSec: surgewaveNativeResult.ProducerMsgPerSec,
            ProducerMBPerSec:  surgewaveNativeResult.ProducerMBPerSec,
            ConsumerMsgPerSec: surgewaveNativeResult.ConsumerMsgPerSec,
            ConsumerMBPerSec:  surgewaveNativeResult.ConsumerMBPerSec));

        Console.WriteLine();
        Console.WriteLine("Run 'benchmark-kafka' or 'compare' to view a cross-platform comparison table.");
    }
}
