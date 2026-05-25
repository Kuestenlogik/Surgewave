using Kuestenlogik.Surgewave.Benchmarks.Comparison.Infrastructure;
using Kuestenlogik.Surgewave.Benchmarks.Comparison.Models;
using Kuestenlogik.Surgewave.Runtime;

namespace Kuestenlogik.Surgewave.Benchmarks.Comparison.Scenarios;

/// <summary>
/// Compares single-message round-trip latency: P50, P90, P99 for both produce and consume
/// across all enabled platforms. Uses synchronous per-message acknowledgment to measure true latency.
/// </summary>
public sealed class LatencyComparison : ComparisonScenario
{
    public override string Name => "Latency";

    public override string Description =>
        "Single-message round-trip latency comparison (P50/P90/P99)";

    public override async Task<ComparisonResult> RunSurgewaveAsync(BenchmarkParams p, CancellationToken ct)
    {
        var count = Math.Min(p.MessageCount, 10_000);
        return await RunSurgewaveLatencyAsync(count, p.MessageSizeBytes, ct: ct);
    }

    public override async Task<ComparisonResult> RunKafkaAsync(BenchmarkParams p, CancellationToken ct)
    {
        var count = Math.Min(p.MessageCount, 10_000);
        var bootstrap = await GetKafkaBootstrapAsync(p);
        return await RunKafkaLatencyAsync(bootstrap, count, p.MessageSizeBytes, ct: ct);
    }

    /// <summary>
    /// Override to dispatch latency-specific implementations per platform.
    /// </summary>
    protected override async Task<ComparisonResult> RunPlatformAsync(
        BenchmarkPlatform platform, BenchmarkParams p, CancellationToken ct)
    {
        var count = Math.Min(p.MessageCount, 10_000);

        return platform switch
        {
            BenchmarkPlatform.SurgewaveEmbeddedNative =>
                await RunSurgewaveLatencyAsync(count, p.MessageSizeBytes, BenchmarkPlatform.SurgewaveEmbeddedNative, ct),

            BenchmarkPlatform.SurgewaveEmbeddedKafka =>
                await RunEmbeddedKafkaLatencyAsync(count, p.MessageSizeBytes, ct),

            BenchmarkPlatform.SurgewaveStandaloneNative =>
                await RunStandaloneNativeLatencyAsync(p, count, ct),

            BenchmarkPlatform.SurgewaveStandaloneKafka =>
                await RunKafkaLatencyWithBootstrapAsync(p.SurgewaveStandaloneAddress, count, p.MessageSizeBytes,
                    BenchmarkPlatform.SurgewaveStandaloneKafka, ct),

            BenchmarkPlatform.SurgewaveContainerNative =>
                await RunContainerNativeLatencyAsync(p, count, ct),

            BenchmarkPlatform.SurgewaveContainerKafka =>
                await RunKafkaLatencyWithBootstrapAsync(
                    await ContainerManager.GetSurgewaveContainerBootstrapAsync(p.SurgewaveContainerImage),
                    count, p.MessageSizeBytes, BenchmarkPlatform.SurgewaveContainerKafka, ct),

            BenchmarkPlatform.ApacheKafkaContainer =>
                await RunKafkaLatencyWithBootstrapAsync(
                    await ContainerManager.GetOrStartKafkaAsync(p.KafkaBootstrap, p.KafkaContainerImage),
                    count, p.MessageSizeBytes, BenchmarkPlatform.ApacheKafkaContainer, ct),

            BenchmarkPlatform.RedpandaContainer =>
                await RunKafkaLatencyWithBootstrapAsync(
                    await ContainerManager.GetOrStartRedpandaAsync(image: p.RedpandaContainerImage),
                    count, p.MessageSizeBytes, BenchmarkPlatform.RedpandaContainer, ct),

            _ => throw new ArgumentOutOfRangeException(nameof(platform), platform, "Unknown platform")
        };
    }

    private static async Task<ComparisonResult> RunEmbeddedKafkaLatencyAsync(
        int messageCount, int messageSizeBytes, CancellationToken ct)
    {
        await using var surgewave = await SurgewaveRuntime.CreateBuilder()
            .WithPort(0)
            .WithAutoCreateTopics(true)
            .WithPartitions(1)
            .Build()
            .StartAsync();

        var bootstrap = $"{surgewave.Host}:{surgewave.Port}";
        return await RunKafkaLatencyAsync(bootstrap, messageCount, messageSizeBytes,
            BenchmarkPlatform.SurgewaveEmbeddedKafka, ct);
    }

    private static async Task<ComparisonResult> RunStandaloneNativeLatencyAsync(
        BenchmarkParams p, int messageCount, CancellationToken ct)
    {
        if (!await ContainerManager.IsKafkaAvailableAsync(p.SurgewaveStandaloneAddress, TimeSpan.FromSeconds(3)))
            throw new InvalidOperationException($"Surgewave standalone not available at {p.SurgewaveStandaloneAddress}");

        var parts = p.SurgewaveStandaloneAddress.Split(':');
        return await RunNativeClientLatencyAsync(
            parts[0], int.Parse(parts[1]), messageCount, p.MessageSizeBytes,
            BenchmarkPlatform.SurgewaveStandaloneNative, ct);
    }

    private static async Task<ComparisonResult> RunContainerNativeLatencyAsync(
        BenchmarkParams p, int messageCount, CancellationToken ct)
    {
        var bootstrap = await ContainerManager.GetSurgewaveContainerBootstrapAsync(p.SurgewaveContainerImage);
        var parts = bootstrap.Split(':');
        return await RunNativeClientLatencyAsync(
            parts[0], int.Parse(parts[1]), messageCount, p.MessageSizeBytes,
            BenchmarkPlatform.SurgewaveContainerNative, ct);
    }

    private static async Task<ComparisonResult> RunKafkaLatencyWithBootstrapAsync(
        string bootstrap, int messageCount, int messageSizeBytes,
        BenchmarkPlatform platform, CancellationToken ct)
    {
        return await RunKafkaLatencyAsync(bootstrap, messageCount, messageSizeBytes, platform, ct);
    }
}
