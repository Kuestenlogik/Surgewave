using Kuestenlogik.Surgewave.Runtime;

namespace Kuestenlogik.Surgewave.Benchmarks.Comparison;

/// <summary>
/// Embedded Surgewave benchmark — runs both the Kafka-protocol and Native-protocol
/// variants against an in-process Surgewave broker and saves the results to
///   <c>artifacts/benchmarks/results/surgewave-kafka.json</c>
///   <c>artifacts/benchmarks/results/surgewave-native.json</c>
///
/// Run:
///   dotnet run -- benchmark-surgewave [msgCount] [msgSize]
/// </summary>
public static class SurgewaveBenchmark
{
    public static async Task RunAsync(string[] args)
    {
        var messageCount     = args.Length > 0 && int.TryParse(args[0], out var mc) ? mc : 100_000;
        var messageSizeBytes = args.Length > 1 && int.TryParse(args[1], out var ms) ? ms : 100;

        Console.WriteLine("╔══════════════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║                      Surgewave BENCHMARK (EMBEDDED)                          ║");
        Console.WriteLine("╠══════════════════════════════════════════════════════════════════════════╣");
        Console.WriteLine("║  1. Surgewave + Kafka Protocol  (Confluent.Kafka client, drop-in mode)       ║");
        Console.WriteLine("║  2. Surgewave Native Protocol   (SurgewaveNativeClient, fully optimized)         ║");
        Console.WriteLine("╚══════════════════════════════════════════════════════════════════════════╝");
        Console.WriteLine();
        Console.WriteLine($"  Messages:     {messageCount:N0}");
        Console.WriteLine($"  Message size: {messageSizeBytes} bytes");
        Console.WriteLine($"  Total data:   {(long)messageCount * messageSizeBytes / 1024 / 1024:N1} MB");
        Console.WriteLine();

        var messageValue = new byte[messageSizeBytes];
        Random.Shared.NextBytes(messageValue);

        // ── Start Embedded Surgewave ───────────────────────────────────────────
        Console.WriteLine("Starting embedded Surgewave broker...");
        await using var surgewave = await SurgewaveRuntime.CreateBuilder()
            .WithPort(0)
            .WithAutoCreateTopics(true)
            .WithPartitions(1)
            .Build()
            .StartAsync();
        Console.WriteLine($"  Surgewave started on port {surgewave.Port}");

        // ── Surgewave + Kafka Protocol ─────────────────────────────────────────
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

        // ── Surgewave Native Protocol ──────────────────────────────────────────
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

        // ── Native vs Kafka protocol comparison ────────────────────────────
        Console.WriteLine();
        Console.WriteLine("── Surgewave Protocol Advantage (Native vs Kafka on same broker) ───────────────");
        var prodAdvantage = surgewaveNativeResult.ProducerMsgPerSec > 0
            ? (surgewaveNativeResult.ProducerMsgPerSec - surgewaveKafkaResult.ProducerMsgPerSec)
              / surgewaveKafkaResult.ProducerMsgPerSec * 100
            : 0;
        var consAdvantage = surgewaveNativeResult.ConsumerMsgPerSec > 0
            ? (surgewaveNativeResult.ConsumerMsgPerSec - surgewaveKafkaResult.ConsumerMsgPerSec)
              / surgewaveKafkaResult.ConsumerMsgPerSec * 100
            : 0;

        Console.WriteLine($"  Producer: {(prodAdvantage >= 0 ? "+" : "")}{prodAdvantage:N1}%  (Native over Kafka protocol)");
        Console.WriteLine($"  Consumer: {(consAdvantage >= 0 ? "+" : "")}{consAdvantage:N1}%  (Native over Kafka protocol)");

        Console.WriteLine();
        Console.WriteLine("Run 'compare' to view a cross-platform comparison table.");
    }
}
