using Kuestenlogik.Surgewave.Benchmarks.Comparison.Infrastructure;
using Kuestenlogik.Surgewave.Benchmarks.Comparison.Models;
using Kuestenlogik.Surgewave.Runtime;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Benchmarks.Comparison.Scenarios;

/// <summary>
/// Compares throughput scaling with different message sizes across all enabled platforms.
/// Same message count, swept across sizes: 100B, 1KB, 10KB, 100KB.
/// Shows how each platform handles increasing message sizes.
/// </summary>
public sealed class MessageSizeComparison : ComparisonScenario
{
    private static readonly (int Bytes, string Label)[] MessageSizes =
    [
        (100, "100B"),
        (1024, "1KB"),
        (10 * 1024, "10KB"),
        (100 * 1024, "100KB")
    ];

    public override string Name => "Message Size Impact";

    public override string Description =>
        "Throughput variation across message sizes (100B, 1KB, 10KB, 100KB)";

    public override Task<ComparisonResult> RunSurgewaveAsync(BenchmarkParams p, CancellationToken ct)
    {
        return RunSurgewaveThroughputAsync(p.MessageCount, p.MessageSizeBytes, p.BatchSize, ct: ct);
    }

    public override Task<ComparisonResult> RunKafkaAsync(BenchmarkParams p, CancellationToken ct)
    {
        return RunKafkaThroughputAsync(p.KafkaBootstrap, p.MessageCount, p.MessageSizeBytes, p.BatchSize, ct: ct);
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

        foreach (var (sizeBytes, sizeLabel) in MessageSizes)
        {
            // Reduce message count for large messages to keep run time reasonable
            var adjustedCount = sizeBytes >= 10_000 ? Math.Min(p.MessageCount, 10_000) : p.MessageCount;

            AnsiConsole.MarkupLine($"  Message size [bold]{sizeLabel}[/] ({adjustedCount:N0} messages):");
            var results = new List<ComparisonResult>();

            foreach (var platform in Enum.GetValues<BenchmarkPlatform>())
            {
                if (!p.IsPlatformEnabled(platform))
                    continue;

                try
                {
                    var result = await RunPlatformThroughputAsync(platform, p, adjustedCount, sizeBytes, bootstraps, ct);
                    results.Add(result);
                    AnsiConsole.MarkupLine($"    [{platform.Color()}]{platform.DisplayName()}[/]: {result.ProduceThroughputMbPerSec:N1} MB/s");

                    if (!bestPerPlatform.TryGetValue(platform, out var best) ||
                        result.ProduceThroughputMbPerSec > best.ProduceThroughputMbPerSec)
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
                Label = sizeLabel,
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

    private static async Task<ComparisonResult> RunPlatformThroughputAsync(
        BenchmarkPlatform platform, BenchmarkParams p, int messageCount, int messageSizeBytes,
        Dictionary<BenchmarkPlatform, string> bootstraps, CancellationToken ct)
    {
        return platform switch
        {
            BenchmarkPlatform.SurgewaveEmbeddedNative =>
                await RunSurgewaveThroughputAsync(messageCount, messageSizeBytes, p.BatchSize,
                    BenchmarkPlatform.SurgewaveEmbeddedNative, ct),

            BenchmarkPlatform.SurgewaveEmbeddedKafka =>
                await RunEmbeddedKafkaThroughputAsync(messageCount, messageSizeBytes, p.BatchSize, ct),

            BenchmarkPlatform.SurgewaveStandaloneNative =>
                await RunStandaloneNativeThroughputAsync(bootstraps[platform], messageCount, messageSizeBytes, p.BatchSize, ct),

            BenchmarkPlatform.SurgewaveStandaloneKafka or
            BenchmarkPlatform.SurgewaveContainerKafka or
            BenchmarkPlatform.ApacheKafkaContainer or
            BenchmarkPlatform.RedpandaContainer =>
                await RunKafkaThroughputAsync(bootstraps[platform], messageCount, messageSizeBytes, p.BatchSize, platform, ct),

            BenchmarkPlatform.SurgewaveContainerNative =>
                await RunStandaloneNativeThroughputAsync(bootstraps[platform], messageCount, messageSizeBytes, p.BatchSize, ct,
                    BenchmarkPlatform.SurgewaveContainerNative),

            _ => throw new ArgumentOutOfRangeException(nameof(platform))
        };
    }

    private static async Task<ComparisonResult> RunEmbeddedKafkaThroughputAsync(
        int messageCount, int messageSizeBytes, int batchSize, CancellationToken ct)
    {
        await using var surgewave = await SurgewaveRuntime.CreateBuilder()
            .WithPort(0)
            .WithAutoCreateTopics(true)
            .WithPartitions(1)
            .Build()
            .StartAsync();

        var bootstrap = $"{surgewave.Host}:{surgewave.Port}";
        return await RunKafkaThroughputAsync(bootstrap, messageCount, messageSizeBytes, batchSize,
            BenchmarkPlatform.SurgewaveEmbeddedKafka, ct);
    }

    private static async Task<ComparisonResult> RunStandaloneNativeThroughputAsync(
        string address, int messageCount, int messageSizeBytes, int batchSize,
        CancellationToken ct, BenchmarkPlatform platformType = BenchmarkPlatform.SurgewaveStandaloneNative)
    {
        var parts = address.Split(':');
        return await RunNativeClientThroughputAsync(
            parts[0], int.Parse(parts[1]), messageCount, messageSizeBytes, batchSize, platformType, ct);
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
