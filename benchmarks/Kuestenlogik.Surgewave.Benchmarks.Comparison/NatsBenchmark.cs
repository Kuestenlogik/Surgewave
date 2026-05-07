using System.Diagnostics;
using Kuestenlogik.Surgewave.Runtime;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace Kuestenlogik.Surgewave.Benchmarks.Comparison;

/// <summary>
/// Competitor-first NATS JetStream benchmark.
///
/// Default: benchmarks only NATS and saves the result to
///   <c>artifacts/benchmarks/results/nats.json</c>.
///
/// With <c>--include-surgewave</c>: also benchmarks embedded Surgewave (both Kafka-protocol
/// and Native-protocol variants) and saves those results separately.
///
/// Run:
///   dotnet run -- benchmark-nats [msgCount] [msgSize] [natsUrl] [--include-surgewave]
///
/// Start NATS with JetStream:
///   docker run -d --name nats -p 4222:4222 nats:latest -js
/// </summary>
public static class NatsBenchmark
{
    public static async Task RunAsync(string[] args)
    {
        var messageCount     = args.Length > 0 && int.TryParse(args[0], out var mc) ? mc : 100_000;
        var messageSizeBytes = args.Length > 1 && int.TryParse(args[1], out var ms) ? ms : 100;
        var natsUrl          = args.Length > 2 && !args[2].StartsWith('-') ? args[2] : "nats://localhost:4222";
        var includeSurgewave     = args.Any(a => a.Equals("--include-surgewave", StringComparison.OrdinalIgnoreCase));

        Console.WriteLine("╔══════════════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║                      NATS JETSTREAM BENCHMARK                            ║");
        Console.WriteLine("╠══════════════════════════════════════════════════════════════════════════╣");
        Console.WriteLine("║  NATS broker + NATS.Net client with JetStream (persistent stream)        ║");
        if (includeSurgewave)
            Console.WriteLine("║  + Surgewave embedded (Kafka protocol + Native protocol)                     ║");
        Console.WriteLine("╚══════════════════════════════════════════════════════════════════════════╝");
        Console.WriteLine();
        Console.WriteLine($"  Messages:      {messageCount:N0}");
        Console.WriteLine($"  Message size:  {messageSizeBytes} bytes");
        Console.WriteLine($"  Total data:    {(long)messageCount * messageSizeBytes / 1024 / 1024:N1} MB");
        Console.WriteLine($"  NATS URL:      {natsUrl}");
        Console.WriteLine();

        var messageValue = new byte[messageSizeBytes];
        Random.Shared.NextBytes(messageValue);

        // ── NATS JetStream ─────────────────────────────────────────────────
        Console.WriteLine("── NATS JetStream (NATS broker + NATS.Net client) ──────────────────────────");
        try
        {
            var result = await RunNatsBenchmarkAsync(natsUrl, messageCount, messageSizeBytes, messageValue);

            Console.WriteLine($"  Producer: {result.ProducerMsgPerSec:N0} msg/sec  ({result.ProducerMBPerSec:N1} MB/sec)");
            Console.WriteLine($"  Consumer: {result.ConsumerMsgPerSec:N0} msg/sec  ({result.ConsumerMBPerSec:N1} MB/sec)");

            BenchmarkResultStore.Save(new BenchmarkResultEntry(
                Platform:         "nats",
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
            Console.WriteLine($"  SKIPPED: NATS broker not available at {natsUrl}");
            Console.WriteLine($"  Error:   {ex.Message}");
            Console.WriteLine($"  Tip:     docker run -d --name nats -p 4222:4222 nats:latest -js");
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
        Console.WriteLine("Run 'compare' to view a cross-platform comparison table.");
    }

    // ── NATS execution helper ─────────────────────────────────────────────────

    private static async Task<BenchmarkResult> RunNatsBenchmarkAsync(
        string natsUrl, int messageCount, int messageSizeBytes, byte[] messageValue)
    {
        var streamName = $"BENCH-{Guid.NewGuid():N}";
        var subject    = $"bench.{streamName.ToLowerInvariant()}";

        var opts = new NatsOpts
        {
            Url = natsUrl,
            ConnectTimeout = TimeSpan.FromSeconds(5),
        };

        await using var nats = new NatsConnection(opts);
        await nats.ConnectAsync();

        var js = new NatsJSContext(nats);

        await js.CreateStreamAsync(new StreamConfig(streamName, [subject])
        {
            Retention = StreamConfigRetention.Limits,
            Storage   = StreamConfigStorage.File,
            MaxMsgs   = messageCount * 2L,
        });

        try
        {
            // Producer benchmark
            var sw = Stopwatch.StartNew();
            for (int i = 0; i < messageCount; i++)
                await js.PublishAsync(subject, messageValue);
            sw.Stop();

            var produceMs        = sw.ElapsedMilliseconds;
            var producerMsgPerSec = messageCount * 1000.0 / produceMs;
            var producerMBPerSec  = (long)messageCount * messageSizeBytes / 1024.0 / 1024.0 * 1000.0 / produceMs;

            // Consumer benchmark: ordered push consumer
            var consumer = await js.CreateOrUpdateConsumerAsync(streamName, new ConsumerConfig
            {
                Name          = $"bench-consumer-{Guid.NewGuid():N}",
                DeliverPolicy = ConsumerConfigDeliverPolicy.All,
                AckPolicy     = ConsumerConfigAckPolicy.None,
                MaxDeliver    = 1,
            });

            sw.Restart();
            var consumed = 0;
            await foreach (var msg in consumer.ConsumeAsync<byte[]>())
            {
                consumed++;
                if (consumed >= messageCount)
                    break;
            }
            sw.Stop();

            var consumeMs        = sw.ElapsedMilliseconds;
            var consumerMsgPerSec = consumed * 1000.0 / consumeMs;
            var consumerMBPerSec  = (long)consumed * messageSizeBytes / 1024.0 / 1024.0 * 1000.0 / consumeMs;

            return new BenchmarkResult(producerMsgPerSec, producerMBPerSec, consumerMsgPerSec, consumerMBPerSec);
        }
        finally
        {
            await js.DeleteStreamAsync(streamName);
        }
    }
}
