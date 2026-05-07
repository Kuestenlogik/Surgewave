using System.Diagnostics;
using Kuestenlogik.Surgewave.Runtime;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Kuestenlogik.Surgewave.Benchmarks.Comparison;

/// <summary>
/// Competitor-first RabbitMQ benchmark.
///
/// Default: benchmarks only RabbitMQ and saves the result to
///   <c>artifacts/benchmarks/results/rabbitmq.json</c>.
///
/// With <c>--include-surgewave</c>: also benchmarks embedded Surgewave (Kafka-protocol
/// and Native-protocol variants) and saves those results separately.
///
/// Run:
///   dotnet run -- benchmark-rabbitmq [msgCount] [msgSize] [host] [--include-surgewave]
///
/// Docker tip:
///   docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:management
/// </summary>
public static class RabbitMqBenchmark
{
    private const int DefaultPort = 5672;

    public static async Task RunAsync(string[] args)
    {
        var messageCount    = args.Length > 0 && int.TryParse(args[0], out var mc) ? mc : 100_000;
        var messageSizeBytes = args.Length > 1 && int.TryParse(args[1], out var ms) ? ms : 100;
        var host            = args.Length > 2 && !args[2].StartsWith('-') ? args[2] : "localhost";
        var includeSurgewave    = args.Any(a => a.Equals("--include-surgewave", StringComparison.OrdinalIgnoreCase));

        Console.WriteLine("╔══════════════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║                     RABBITMQ BENCHMARK                                   ║");
        Console.WriteLine("╠══════════════════════════════════════════════════════════════════════════╣");
        Console.WriteLine("║  RabbitMQ broker + RabbitMQ.Client (AMQP 0.9.1)                         ║");
        if (includeSurgewave)
            Console.WriteLine("║  + Surgewave embedded (Kafka protocol + Native protocol)                     ║");
        Console.WriteLine("╚══════════════════════════════════════════════════════════════════════════╝");
        Console.WriteLine();
        Console.WriteLine($"  Messages:       {messageCount:N0}");
        Console.WriteLine($"  Message size:   {messageSizeBytes} bytes");
        Console.WriteLine($"  Total data:     {(long)messageCount * messageSizeBytes / 1024 / 1024:N1} MB");
        Console.WriteLine($"  RabbitMQ host:  {host}:{DefaultPort}");
        Console.WriteLine();

        var messageValue = new byte[messageSizeBytes];
        Random.Shared.NextBytes(messageValue);

        // ── RabbitMQ ──────────────────────────────────────────────────────────
        Console.WriteLine("── RabbitMQ (RabbitMQ broker + RabbitMQ.Client AMQP 0.9.1) ─────────────────");
        try
        {
            var result = await RunRabbitMqBenchmarkAsync(host, messageCount, messageSizeBytes, messageValue);

            Console.WriteLine($"  Producer: {result.ProducerMsgPerSec:N0} msg/sec  ({result.ProducerMBPerSec:N1} MB/sec)");
            Console.WriteLine($"  Consumer: {result.ConsumerMsgPerSec:N0} msg/sec  ({result.ConsumerMBPerSec:N1} MB/sec)");

            BenchmarkResultStore.Save(new BenchmarkResultEntry(
                Platform:          "rabbitmq",
                Timestamp:         DateTime.UtcNow,
                MessageCount:      messageCount,
                MessageSize:       messageSizeBytes,
                ProducerMsgPerSec: result.ProducerMsgPerSec,
                ProducerMBPerSec:  result.ProducerMBPerSec,
                ConsumerMsgPerSec: result.ConsumerMsgPerSec,
                ConsumerMBPerSec:  result.ConsumerMBPerSec));
        }
        catch (Exception ex)
        {
            Console.WriteLine($"  SKIPPED: RabbitMQ broker not available at {host}:{DefaultPort}");
            Console.WriteLine($"  Error:   {ex.Message}");
            Console.WriteLine($"  Tip:     docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:management");
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
            surgewave.BootstrapServers, messageCount, messageSizeBytes, messageValue, "surgewave-kafka-rmq");
        Console.WriteLine($"  Producer: {surgewaveKafkaResult.ProducerMsgPerSec:N0} msg/sec  ({surgewaveKafkaResult.ProducerMBPerSec:N1} MB/sec)");
        Console.WriteLine($"  Consumer: {surgewaveKafkaResult.ConsumerMsgPerSec:N0} msg/sec  ({surgewaveKafkaResult.ConsumerMBPerSec:N1} MB/sec)");
        BenchmarkResultStore.Save(new BenchmarkResultEntry(
            Platform:          "surgewave-kafka",
            Timestamp:         DateTime.UtcNow,
            MessageCount:      messageCount,
            MessageSize:       messageSizeBytes,
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
            Platform:          "surgewave-native",
            Timestamp:         DateTime.UtcNow,
            MessageCount:      messageCount,
            MessageSize:       messageSizeBytes,
            ProducerMsgPerSec: surgewaveNativeResult.ProducerMsgPerSec,
            ProducerMBPerSec:  surgewaveNativeResult.ProducerMBPerSec,
            ConsumerMsgPerSec: surgewaveNativeResult.ConsumerMsgPerSec,
            ConsumerMBPerSec:  surgewaveNativeResult.ConsumerMBPerSec));

        Console.WriteLine();
        Console.WriteLine("Run 'compare' to view a cross-platform comparison table.");
    }

    // ── RabbitMQ benchmark implementation ─────────────────────────────────────

    private static async Task<BenchmarkResult> RunRabbitMqBenchmarkAsync(
        string host, int messageCount, int messageSize, byte[] messageValue)
    {
        var exchangeName = $"bench-exchange-{Guid.NewGuid():N}";
        var queueName    = $"bench-queue-{Guid.NewGuid():N}";
        var routingKey   = "bench";

        var factory = new ConnectionFactory
        {
            HostName = host,
            Port     = DefaultPort,
        };

        // ── Producer test ──────────────────────────────────────────────────
        double producerMsgPerSec, producerMBPerSec;

        await using (var connection = await factory.CreateConnectionAsync())
        await using (var channel = await connection.CreateChannelAsync())
        {
            // Declare exchange, queue, and binding
            await channel.ExchangeDeclareAsync(
                exchange:    exchangeName,
                type:        ExchangeType.Direct,
                durable:     false,
                autoDelete:  true);

            await channel.QueueDeclareAsync(
                queue:      queueName,
                durable:    false,
                exclusive:  false,
                autoDelete: true);

            await channel.QueueBindAsync(
                queue:      queueName,
                exchange:   exchangeName,
                routingKey: routingKey);

            var sw = Stopwatch.StartNew();
            for (int i = 0; i < messageCount; i++)
            {
                await channel.BasicPublishAsync(
                    exchange:   exchangeName,
                    routingKey: routingKey,
                    body:       messageValue);
            }
            sw.Stop();

            var ms = sw.ElapsedMilliseconds;
            producerMsgPerSec = messageCount * 1000.0 / ms;
            producerMBPerSec  = (long)messageCount * messageSize / 1024.0 / 1024.0 * 1000.0 / ms;
        }

        // ── Consumer test ──────────────────────────────────────────────────
        double consumerMsgPerSec, consumerMBPerSec;

        await using (var connection = await factory.CreateConnectionAsync())
        await using (var channel = await connection.CreateChannelAsync())
        {
            var consumed  = 0;
            var tcs       = new TaskCompletionSource<bool>();
            var sw        = Stopwatch.StartNew();

            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += (_, _) =>
            {
                consumed++;
                if (consumed >= messageCount)
                    tcs.TrySetResult(true);
                return Task.CompletedTask;
            };

            await channel.BasicConsumeAsync(
                queue:       queueName,
                autoAck:     true,
                consumer:    consumer);

            // Wait for all messages or 60 s timeout
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
            cts.Token.Register(() => tcs.TrySetCanceled());
            try
            {
                await tcs.Task;
            }
            catch (OperationCanceledException)
            {
                // Timed out — report with whatever was consumed
            }
            sw.Stop();

            var ms = sw.ElapsedMilliseconds;
            consumerMsgPerSec = consumed * 1000.0 / ms;
            consumerMBPerSec  = (long)consumed * messageSize / 1024.0 / 1024.0 * 1000.0 / ms;
        }

        return new BenchmarkResult(producerMsgPerSec, producerMBPerSec, consumerMsgPerSec, consumerMBPerSec);
    }
}
