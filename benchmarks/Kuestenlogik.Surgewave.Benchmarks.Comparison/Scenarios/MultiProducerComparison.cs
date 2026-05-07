using System.Diagnostics;
using Confluent.Kafka;
using Kuestenlogik.Surgewave.Benchmarks.Comparison.Infrastructure;
using Kuestenlogik.Surgewave.Benchmarks.Comparison.Models;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Client.Native.Operations.Performance;
using Kuestenlogik.Surgewave.Runtime;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Benchmarks.Comparison.Scenarios;

/// <summary>
/// Compares how throughput scales with concurrent producers across all enabled platforms.
/// Tests with 1, 3, 5, and 10 producers running in parallel.
/// </summary>
public sealed class MultiProducerComparison : ComparisonScenario
{
    private static readonly int[] ProducerCounts = [1, 3, 5, 10];

    public override string Name => "Multi-Producer Scaling";

    public override string Description =>
        "Throughput scaling with concurrent producers (1, 3, 5, 10)";

    public override Task<ComparisonResult> RunSurgewaveAsync(BenchmarkParams p, CancellationToken ct)
    {
        return RunSurgewaveMultiProducerAsync(p.MessageCount, p.MessageSizeBytes, p.BatchSize, 1,
            BenchmarkPlatform.SurgewaveEmbeddedNative, ct);
    }

    public override Task<ComparisonResult> RunKafkaAsync(BenchmarkParams p, CancellationToken ct)
    {
        return RunKafkaMultiProducerAsync(p.KafkaBootstrap, p.MessageCount, p.MessageSizeBytes, p.BatchSize, 1,
            BenchmarkPlatform.ApacheKafkaContainer, ct);
    }

    public override async Task<ComparisonReport> RunAsync(BenchmarkParams p, CancellationToken ct)
    {
        AnsiConsole.Write(new Rule($"[bold cyan]{Name}[/]"));
        AnsiConsole.MarkupLine($"  [dim]{Description}[/]");
        AnsiConsole.WriteLine();

        var subResults = new List<ComparisonSubResult>();
        var bestPerPlatform = new Dictionary<BenchmarkPlatform, ComparisonResult>();

        // Pre-resolve bootstrap addresses for enabled platforms
        var bootstraps = await ResolveBootstrapsAsync(p);

        foreach (var producerCount in ProducerCounts)
        {
            var messagesPerProducer = p.MessageCount / producerCount;
            AnsiConsole.MarkupLine($"  [bold]{producerCount}[/] producer(s), {messagesPerProducer:N0} msg each:");
            var results = new List<ComparisonResult>();

            foreach (var platform in Enum.GetValues<BenchmarkPlatform>())
            {
                if (!p.IsPlatformEnabled(platform))
                    continue;

                try
                {
                    var result = await RunPlatformMultiProducerAsync(platform, p, producerCount, bootstraps, ct);
                    results.Add(result);
                    AnsiConsole.MarkupLine($"    [{platform.Color()}]{platform.DisplayName()}[/]: {result.ProduceThroughputMsgPerSec:N0} msg/s (aggregate)");

                    if (!bestPerPlatform.TryGetValue(platform, out var best) ||
                        result.ProduceThroughputMsgPerSec > best.ProduceThroughputMsgPerSec)
                    {
                        bestPerPlatform[platform] = result;
                    }
                }
                catch (Exception ex)
                {
                    AnsiConsole.MarkupLine($"    [dim]{platform.DisplayName()} skipped: {ex.Message}[/]");
                }
            }

            subResults.Add(new ComparisonSubResult
            {
                Label = $"{producerCount} producer(s)",
                Results = results
            });
        }

        AnsiConsole.WriteLine();

        return new ComparisonReport
        {
            ScenarioName = Name,
            Description = Description,
            Results = bestPerPlatform.Values.ToList(),
            SubResults = subResults
        };
    }

    private static async Task<ComparisonResult> RunPlatformMultiProducerAsync(
        BenchmarkPlatform platform, BenchmarkParams p, int producerCount,
        Dictionary<BenchmarkPlatform, string> bootstraps, CancellationToken ct)
    {
        return platform switch
        {
            BenchmarkPlatform.SurgewaveEmbeddedNative =>
                await RunSurgewaveMultiProducerAsync(p.MessageCount, p.MessageSizeBytes, p.BatchSize, producerCount,
                    BenchmarkPlatform.SurgewaveEmbeddedNative, ct),

            BenchmarkPlatform.SurgewaveEmbeddedKafka =>
                await RunEmbeddedKafkaMultiProducerAsync(p.MessageCount, p.MessageSizeBytes, p.BatchSize, producerCount, ct),

            BenchmarkPlatform.SurgewaveStandaloneNative =>
                await RunNativeMultiProducerRemoteAsync(bootstraps[platform], p.MessageCount, p.MessageSizeBytes, p.BatchSize,
                    producerCount, BenchmarkPlatform.SurgewaveStandaloneNative, ct),

            BenchmarkPlatform.SurgewaveContainerNative =>
                await RunNativeMultiProducerRemoteAsync(bootstraps[platform], p.MessageCount, p.MessageSizeBytes, p.BatchSize,
                    producerCount, BenchmarkPlatform.SurgewaveContainerNative, ct),

            BenchmarkPlatform.SurgewaveStandaloneKafka or
            BenchmarkPlatform.SurgewaveContainerKafka or
            BenchmarkPlatform.ApacheKafkaContainer or
            BenchmarkPlatform.RedpandaContainer =>
                await RunKafkaMultiProducerAsync(bootstraps[platform], p.MessageCount, p.MessageSizeBytes, p.BatchSize,
                    producerCount, platform, ct),

            _ => throw new ArgumentOutOfRangeException(nameof(platform))
        };
    }

    private static async Task<ComparisonResult> RunEmbeddedKafkaMultiProducerAsync(
        int totalMessages, int messageSizeBytes, int batchSize, int producerCount, CancellationToken ct)
    {
        await using var surgewave = await SurgewaveRuntime.CreateBuilder()
            .WithPort(0)
            .WithAutoCreateTopics(true)
            .WithPartitions(producerCount)
            .Build()
            .StartAsync();

        var bootstrap = $"{surgewave.Host}:{surgewave.Port}";
        return await RunKafkaMultiProducerAsync(bootstrap, totalMessages, messageSizeBytes, batchSize,
            producerCount, BenchmarkPlatform.SurgewaveEmbeddedKafka, ct);
    }

    private static async Task<ComparisonResult> RunSurgewaveMultiProducerAsync(
        int totalMessages, int messageSizeBytes, int batchSize, int producerCount,
        BenchmarkPlatform platformType, CancellationToken ct)
    {
        var payload = new byte[messageSizeBytes];
        Random.Shared.NextBytes(payload);
        var messagesPerProducer = totalMessages / producerCount;

        await using var surgewave = await SurgewaveRuntime.CreateBuilder()
            .WithPort(0)
            .WithAutoCreateTopics(true)
            .WithPartitions(producerCount)
            .Build()
            .StartAsync();

        var topicName = $"surgewave-mp-{Guid.NewGuid():N}";

        await using var client = new SurgewaveNativeClient(surgewave.Host, surgewave.Port);
        await client.ConnectAsync();
        await client.Topics.CreateAsync(topicName, producerCount);

        var sw = Stopwatch.StartNew();

        var tasks = Enumerable.Range(0, producerCount).Select(async partitionId =>
        {
            await using var partClient = new SurgewaveNativeClient(surgewave.Host, surgewave.Port);
            await partClient.ConnectAsync();

            await using var producer = new SurgewaveBatchingProducer(
                partClient, topicName, partitionId,
                maxBatchSize: batchSize,
                lingerTime: TimeSpan.FromMilliseconds(5));

            for (int i = 0; i < messagesPerProducer; i++)
            {
                ct.ThrowIfCancellationRequested();
                await producer.ProduceAsync(null, payload);
            }
            await producer.FlushAsync();
        }).ToArray();

        await Task.WhenAll(tasks);
        sw.Stop();

        var produceMs = Math.Max(1, sw.ElapsedMilliseconds);
        var totalProduced = messagesPerProducer * producerCount;
        var produceMsgPerSec = totalProduced * 1000.0 / produceMs;
        var produceMBPerSec = (long)totalProduced * messageSizeBytes / 1024.0 / 1024.0 * 1000.0 / produceMs;

        return new ComparisonResult
        {
            Platform = platformType.DisplayName(),
            PlatformType = platformType,
            ProduceThroughputMsgPerSec = produceMsgPerSec,
            ProduceThroughputMbPerSec = produceMBPerSec,
            ConsumeThroughputMsgPerSec = 0, // Multi-producer scenario focuses on produce
            ConsumeThroughputMbPerSec = 0,
            TotalBytesProduced = (long)totalProduced * messageSizeBytes,
            Duration = sw.Elapsed
        };
    }

    private static async Task<ComparisonResult> RunNativeMultiProducerRemoteAsync(
        string address, int totalMessages, int messageSizeBytes, int batchSize,
        int producerCount, BenchmarkPlatform platformType, CancellationToken ct)
    {
        var payload = new byte[messageSizeBytes];
        Random.Shared.NextBytes(payload);
        var messagesPerProducer = totalMessages / producerCount;
        var topicName = $"surgewave-mp-{Guid.NewGuid():N}";
        var parts = address.Split(':');
        var host = parts[0];
        var port = int.Parse(parts[1]);

        await using var client = new SurgewaveNativeClient(host, port);
        await client.ConnectAsync();
        await client.Topics.CreateAsync(topicName, producerCount);

        var sw = Stopwatch.StartNew();

        var tasks = Enumerable.Range(0, producerCount).Select(async partitionId =>
        {
            await using var partClient = new SurgewaveNativeClient(host, port);
            await partClient.ConnectAsync();

            await using var producer = new SurgewaveBatchingProducer(
                partClient, topicName, partitionId,
                maxBatchSize: batchSize,
                lingerTime: TimeSpan.FromMilliseconds(5));

            for (int i = 0; i < messagesPerProducer; i++)
            {
                ct.ThrowIfCancellationRequested();
                await producer.ProduceAsync(null, payload);
            }
            await producer.FlushAsync();
        }).ToArray();

        await Task.WhenAll(tasks);
        sw.Stop();

        var produceMs = Math.Max(1, sw.ElapsedMilliseconds);
        var totalProduced = messagesPerProducer * producerCount;
        var produceMsgPerSec = totalProduced * 1000.0 / produceMs;
        var produceMBPerSec = (long)totalProduced * messageSizeBytes / 1024.0 / 1024.0 * 1000.0 / produceMs;

        return new ComparisonResult
        {
            Platform = platformType.DisplayName(),
            PlatformType = platformType,
            ProduceThroughputMsgPerSec = produceMsgPerSec,
            ProduceThroughputMbPerSec = produceMBPerSec,
            ConsumeThroughputMsgPerSec = 0,
            ConsumeThroughputMbPerSec = 0,
            TotalBytesProduced = (long)totalProduced * messageSizeBytes,
            Duration = sw.Elapsed
        };
    }

    private static async Task<ComparisonResult> RunKafkaMultiProducerAsync(
        string bootstrapServers, int totalMessages, int messageSizeBytes, int batchSize,
        int producerCount, BenchmarkPlatform platformType, CancellationToken ct)
    {
        var payload = new byte[messageSizeBytes];
        Random.Shared.NextBytes(payload);
        var messagesPerProducer = totalMessages / producerCount;
        var topicName = $"kafka-mp-{Guid.NewGuid():N}";

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            Acks = Acks.Leader,
            LingerMs = 5,
            BatchSize = 65536,
            QueueBufferingMaxMessages = 2000000,
            QueueBufferingMaxKbytes = 2097152,
        };

        var sw = Stopwatch.StartNew();

        var tasks = Enumerable.Range(0, producerCount).Select(producerId =>
        {
            return Task.Run(() =>
            {
                using var producer = new ProducerBuilder<Null, byte[]>(producerConfig).Build();
                for (int i = 0; i < messagesPerProducer; i++)
                {
                    ct.ThrowIfCancellationRequested();
                    producer.Produce(topicName, new Message<Null, byte[]> { Value = payload });
                }
                producer.Flush(TimeSpan.FromSeconds(60));
            }, ct);
        }).ToArray();

        await Task.WhenAll(tasks);
        sw.Stop();

        var produceMs = Math.Max(1, sw.ElapsedMilliseconds);
        var totalProduced = messagesPerProducer * producerCount;
        var produceMsgPerSec = totalProduced * 1000.0 / produceMs;
        var produceMBPerSec = (long)totalProduced * messageSizeBytes / 1024.0 / 1024.0 * 1000.0 / produceMs;

        return new ComparisonResult
        {
            Platform = platformType.DisplayName(),
            PlatformType = platformType,
            ProduceThroughputMsgPerSec = produceMsgPerSec,
            ProduceThroughputMbPerSec = produceMBPerSec,
            ConsumeThroughputMsgPerSec = 0,
            ConsumeThroughputMbPerSec = 0,
            TotalBytesProduced = (long)totalProduced * messageSizeBytes,
            Duration = sw.Elapsed
        };
    }

    private static async Task<Dictionary<BenchmarkPlatform, string>> ResolveBootstrapsAsync(BenchmarkParams p)
    {
        var bootstraps = new Dictionary<BenchmarkPlatform, string>();

        foreach (var platform in Enum.GetValues<BenchmarkPlatform>())
        {
            if (!p.IsPlatformEnabled(platform) || platform.IsEmbedded())
                continue;

            try
            {
                var bootstrap = await GetBootstrapForPlatformAsync(platform, p);
                bootstraps[platform] = bootstrap;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  {platform.DisplayName()} bootstrap failed: {ex.Message}");
            }
        }

        return bootstraps;
    }
}
