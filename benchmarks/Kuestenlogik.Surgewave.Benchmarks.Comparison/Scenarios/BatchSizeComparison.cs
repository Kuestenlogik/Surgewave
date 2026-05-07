using Kuestenlogik.Surgewave.Benchmarks.Comparison.Infrastructure;
using Kuestenlogik.Surgewave.Benchmarks.Comparison.Models;
using Kuestenlogik.Surgewave.Runtime;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Benchmarks.Comparison.Scenarios;

/// <summary>
/// Compares how batch size affects throughput across all enabled platforms.
/// Same total messages, swept across batch sizes: 1, 10, 100, 1000, 10000.
/// </summary>
public sealed class BatchSizeComparison : ComparisonScenario
{
    private static readonly int[] BatchSizes = [1, 10, 100, 1000, 10_000];

    public override string Name => "Batch Size Impact";

    public override string Description =>
        "Throughput variation across batch sizes (1, 10, 100, 1000, 10000)";

    public override Task<ComparisonResult> RunSurgewaveAsync(BenchmarkParams p, CancellationToken ct)
    {
        // Not used directly - RunAsync is overridden
        return RunSurgewaveThroughputAsync(p.MessageCount, p.MessageSizeBytes, p.BatchSize, ct: ct);
    }

    public override Task<ComparisonResult> RunKafkaAsync(BenchmarkParams p, CancellationToken ct)
    {
        // Not used directly - RunAsync is overridden
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

        foreach (var batchSize in BatchSizes)
        {
            AnsiConsole.MarkupLine($"  Batch size [bold]{batchSize:N0}[/]:");
            var results = new List<ComparisonResult>();

            foreach (var platform in Enum.GetValues<BenchmarkPlatform>())
            {
                if (!p.IsPlatformEnabled(platform))
                    continue;

                try
                {
                    var result = await RunPlatformThroughputAsync(platform, p, batchSize, bootstraps, ct);
                    results.Add(result);
                    AnsiConsole.MarkupLine($"    [{platform.Color()}]{platform.DisplayName()}[/]: {result.ProduceThroughputMsgPerSec:N0} msg/s");

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
                Label = $"Batch {batchSize:N0}",
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
        BenchmarkPlatform platform, BenchmarkParams p, int batchSize,
        Dictionary<BenchmarkPlatform, string> bootstraps, CancellationToken ct)
    {
        return platform switch
        {
            BenchmarkPlatform.SurgewaveEmbeddedNative =>
                await RunSurgewaveThroughputAsync(p.MessageCount, p.MessageSizeBytes, batchSize,
                    BenchmarkPlatform.SurgewaveEmbeddedNative, ct),

            BenchmarkPlatform.SurgewaveEmbeddedKafka =>
                await RunEmbeddedKafkaThroughputAsync(p.MessageCount, p.MessageSizeBytes, batchSize, ct),

            BenchmarkPlatform.SurgewaveStandaloneNative =>
                await RunStandaloneNativeThroughputAsync(bootstraps[platform], p.MessageCount, p.MessageSizeBytes, batchSize, ct),

            BenchmarkPlatform.SurgewaveStandaloneKafka or
            BenchmarkPlatform.SurgewaveContainerKafka or
            BenchmarkPlatform.ApacheKafkaContainer or
            BenchmarkPlatform.RedpandaContainer =>
                await RunKafkaThroughputAsync(bootstraps[platform], p.MessageCount, p.MessageSizeBytes, batchSize, platform, ct),

            BenchmarkPlatform.SurgewaveContainerNative =>
                await RunStandaloneNativeThroughputAsync(bootstraps[platform], p.MessageCount, p.MessageSizeBytes, batchSize, ct,
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
