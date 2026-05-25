using System.Diagnostics;
using System.Globalization;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Client.Native.Operations.Performance;
using Kuestenlogik.Surgewave.Runtime;

namespace Kuestenlogik.Surgewave.Benchmarks.Integration;

/// <summary>
/// Embedded broker throughput benchmark comparing File vs Memory vs Arrow storage.
/// Uses EmbeddedSurgewave which runs the broker in-process.
///
/// Run with: dotnet run -- embedded [msgCount] [msgSize] [batchSize] [storageMode]
///
/// StorageEngine: "file", "memory", "arrow", "arrownocompress", "both" (file+memory), or "all"
///
/// Note on Embedded vs Standalone Performance:
/// The embedded broker uses the identical SurgewaveBroker code as the standalone.
/// Performance should be equivalent because:
/// - Same networking stack (TCP loopback)
/// - Same storage engine (memory-mapped files or in-memory)
/// - Same serialization code
///
/// The only overhead is in-process startup/shutdown, not throughput.
/// </summary>
public static class EmbeddedThroughputBenchmark
{
    public static async Task RunAsync(string[] args)
    {
        var messageCount = args.Length > 0 ? int.Parse(args[0]) : 100_000;
        var messageSizeBytes = args.Length > 1 ? int.Parse(args[1]) : 100;
        var batchSize = args.Length > 2 ? int.Parse(args[2]) : 1000;
        var storageModeStr = args.Length > 3 ? args[3].ToLowerInvariant() : "both";
        var topicName = "embedded-benchmark";

        Console.WriteLine("Embedded Surgewave Throughput Benchmark");
        Console.WriteLine("===================================");
        Console.WriteLine($"Messages:     {messageCount:N0}");
        Console.WriteLine($"Message size: {messageSizeBytes} bytes");
        Console.WriteLine($"Batch size:   {batchSize}");
        Console.WriteLine($"Total data:   {(long)messageCount * messageSizeBytes / 1024 / 1024:N1} MB");
        Console.WriteLine();

        // Determine which modes to run
        var runFile = storageModeStr is "file" or "both" or "all";
        var runMemory = storageModeStr is "memory" or "both" or "all";
        var runArrow = storageModeStr is "arrow" or "all";
        var runArrowNoCompress = storageModeStr is "arrownocompress" or "all";

        // Store results for comparison
        var results = new List<(string Mode, double ProduceMsgPerSec, double ProduceMBPerSec, double ConsumeMsgPerSec, double ConsumeMBPerSec, long StartupMs)>();

        if (runFile)
        {
            var result = await RunBenchmarkAsync(StorageEngines.File, messageCount, messageSizeBytes, batchSize, topicName);
            results.Add(("File", result.ProduceMsgPerSec, result.ProduceMBPerSec, result.ConsumeMsgPerSec, result.ConsumeMBPerSec, result.StartupMs));
        }

        if (runMemory)
        {
            var result = await RunBenchmarkAsync(StorageEngines.Memory, messageCount, messageSizeBytes, batchSize, topicName);
            results.Add(("Memory", result.ProduceMsgPerSec, result.ProduceMBPerSec, result.ConsumeMsgPerSec, result.ConsumeMBPerSec, result.StartupMs));
        }

        if (runArrow)
        {
            var result = await RunBenchmarkAsync("arrow", messageCount, messageSizeBytes, batchSize, topicName);
            results.Add(("Arrow", result.ProduceMsgPerSec, result.ProduceMBPerSec, result.ConsumeMsgPerSec, result.ConsumeMBPerSec, result.StartupMs));
        }

        if (runArrowNoCompress)
        {
            var result = await RunBenchmarkAsync("arrow-nocompression", messageCount, messageSizeBytes, batchSize, topicName);
            results.Add(("ArrowNC", result.ProduceMsgPerSec, result.ProduceMBPerSec, result.ConsumeMsgPerSec, result.ConsumeMBPerSec, result.StartupMs));
        }

        // Print comparison if multiple modes were run
        if (results.Count > 1)
        {
            Console.WriteLine();
            Console.WriteLine("=== STORAGE COMPARISON ===");
            Console.WriteLine("┌─────────┬──────────────┬──────────────┬──────────────┬──────────────┬───────────┐");
            Console.WriteLine("│ Mode    │ Produce/sec  │ Produce MB/s │ Consume/sec  │ Consume MB/s │ Startup   │");
            Console.WriteLine("├─────────┼──────────────┼──────────────┼──────────────┼──────────────┼───────────┤");
            foreach (var (mode, prodMsg, prodMB, consMsg, consMB, startup) in results)
            {
                Console.WriteLine($"│ {mode,-7} │ {prodMsg,10:N0}   │ {prodMB,10:N1}   │ {consMsg,10:N0}   │ {consMB,10:N1}   │ {startup,7} ms │");
            }
            Console.WriteLine("└─────────┴──────────────┴──────────────┴──────────────┴──────────────┴───────────┘");

            // Print speedup comparisons if File is included as baseline
            var fileResult = results.FirstOrDefault(r => r.Mode == "File");
            if (fileResult != default)
            {
                Console.WriteLine();
                Console.WriteLine("Speedup vs File storage:");
                foreach (var (mode, prodMsg, _, consMsg, _, _) in results.Where(r => r.Mode != "File"))
                {
                    var produceSpeedup = prodMsg / fileResult.ProduceMsgPerSec;
                    var consumeSpeedup = consMsg / fileResult.ConsumeMsgPerSec;
                    Console.WriteLine($"  {mode}: Producer {produceSpeedup:F2}x, Consumer {consumeSpeedup:F2}x");
                }
            }
        }
    }

    private static async Task<(double ProduceMsgPerSec, double ProduceMBPerSec, double ConsumeMsgPerSec, double ConsumeMBPerSec, long StartupMs)> RunBenchmarkAsync(
        string storageEngine,
        int messageCount,
        int messageSizeBytes,
        int batchSize,
        string topicName)
    {
        Console.WriteLine($"=== {storageEngine.ToUpperInvariant()} STORAGE ENGINE ===");
        Console.WriteLine();

        // Start embedded broker with specified storage engine
        Console.WriteLine($"Starting embedded broker ({storageEngine} storage)...");
        var startupSw = Stopwatch.StartNew();
        await using var surgewave = await SurgewaveRuntime.CreateBuilder()
            .WithPort(0) // Auto-assign port
            .WithAutoCreateTopics(true)
            .WithPartitions(1)
            .WithStorageEngine(storageEngine)
            .Build()
            .StartAsync();
        startupSw.Stop();
        Console.WriteLine($"Embedded broker started in {startupSw.ElapsedMilliseconds} ms");
        Console.WriteLine($"Listening on port: {surgewave.Port}");
        Console.WriteLine();

        // Prepare test data
        var messageValue = new byte[messageSizeBytes];
        Random.Shared.NextBytes(messageValue);

        // Connect native client
        await using var client = new SurgewaveNativeClient("localhost", surgewave.Port);
        await client.ConnectAsync();

        // Create topic (use unique name to avoid conflicts between runs)
        var uniqueTopicName = $"{topicName}-{storageEngine}";
        await client.Topics.CreateAsync(uniqueTopicName, 1);

        // === PRODUCER BENCHMARK ===
        Console.WriteLine($"=== PRODUCER BENCHMARK ({storageEngine}, Native Protocol, Batched) ===");

        await using var producer = new SurgewaveBatchingProducer(
            client,
            uniqueTopicName,
            partition: 0,
            maxBatchSize: batchSize,
            lingerTime: TimeSpan.FromMilliseconds(5));

        var sw = Stopwatch.StartNew();
        for (int i = 0; i < messageCount; i++)
        {
            await producer.ProduceAsync(null, messageValue);

            if (i > 0 && i % 100_000 == 0)
            {
                Console.WriteLine($"  Produced {i:N0} messages...");
            }
        }

        // Flush and wait for completion
        await producer.FlushAsync();
        sw.Stop();

        var produceMs = sw.ElapsedMilliseconds;
        var produceMsgPerSec = messageCount * 1000.0 / produceMs;
        var produceMBPerSec = (long)messageCount * messageSizeBytes / 1024.0 / 1024.0 * 1000.0 / produceMs;

        Console.WriteLine($"  Time:       {produceMs:N0} ms");
        Console.WriteLine($"  Throughput: {produceMsgPerSec:N0} msg/sec");
        Console.WriteLine($"  Throughput: {produceMBPerSec:N1} MB/sec");
        Console.WriteLine();

        // === CONSUMER BENCHMARK ===
        Console.WriteLine($"=== CONSUMER BENCHMARK ({storageEngine}, Native Protocol) ===");

        sw.Restart();
        var consumed = 0;
        long offset = 0;
        const int maxBytesPerFetch = 1024 * 1024; // 1MB per fetch

        while (consumed < messageCount)
        {
            var result = await client.Messaging.ReceiveAsync(uniqueTopicName, 0, offset, maxBytesPerFetch);
            if (result.Messages.Count == 0)
            {
                // No more messages, give broker time to flush
                await Task.Delay(10);
                continue;
            }

            consumed += result.Messages.Count;
            offset = result.Messages[^1].Offset + 1;

            if (consumed > 0 && consumed % 100_000 == 0)
            {
                Console.WriteLine($"  Consumed {consumed:N0} messages...");
            }
        }
        sw.Stop();

        var consumeMs = sw.ElapsedMilliseconds;
        var consumeMsgPerSec = consumed * 1000.0 / consumeMs;
        var consumeMBPerSec = (long)consumed * messageSizeBytes / 1024.0 / 1024.0 * 1000.0 / consumeMs;

        Console.WriteLine($"  Messages:   {consumed:N0}");
        Console.WriteLine($"  Time:       {consumeMs:N0} ms");
        Console.WriteLine($"  Throughput: {consumeMsgPerSec:N0} msg/sec");
        Console.WriteLine($"  Throughput: {consumeMBPerSec:N1} MB/sec");
        Console.WriteLine();

        // === SUMMARY ===
        Console.WriteLine($"=== SUMMARY ({storageEngine} Storage) ===");
        Console.WriteLine($"Startup time: {startupSw.ElapsedMilliseconds} ms");
        Console.WriteLine($"Producer:     {produceMsgPerSec:N0} msg/sec ({produceMBPerSec:N1} MB/sec)");
        Console.WriteLine($"Consumer:     {consumeMsgPerSec:N0} msg/sec ({consumeMBPerSec:N1} MB/sec)");
        Console.WriteLine();

        return (produceMsgPerSec, produceMBPerSec, consumeMsgPerSec, consumeMBPerSec, startupSw.ElapsedMilliseconds);
    }
}
