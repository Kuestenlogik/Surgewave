using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Exporters.Json;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Loggers;

namespace Kuestenlogik.Surgewave.Benchmarks;

/// <summary>
/// Centralized BenchmarkDotNet configuration for all Surgewave benchmarks.
/// </summary>
public static class BenchmarkConfig
{
    /// <summary>
    /// Path to the artifacts directory for benchmark results.
    /// </summary>
    public static readonly string ArtifactsPath = GetArtifactsPath();

    /// <summary>
    /// Creates the default benchmark configuration.
    /// </summary>
    public static IConfig Create() => ManualConfig
        .Create(DefaultConfig.Instance)
        .WithArtifactsPath(ArtifactsPath)
        .AddDiagnoser(MemoryDiagnoser.Default)
        .AddColumn(RankColumn.Arabic)
        .AddExporter(JsonExporter.Full)
        .AddExporter(MarkdownExporter.GitHub)
        .AddLogger(ConsoleLogger.Default);

    /// <summary>
    /// Creates a quick benchmark configuration for faster iteration.
    /// </summary>
    public static IConfig CreateQuick() => ManualConfig
        .Create(DefaultConfig.Instance)
        .WithArtifactsPath(ArtifactsPath)
        .AddDiagnoser(MemoryDiagnoser.Default)
        .AddJob(Job.ShortRun)
        .AddColumn(RankColumn.Arabic)
        .AddLogger(ConsoleLogger.Default);

    private static string GetArtifactsPath()
    {
        // Find the repo root by looking for the .git folder
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir != null && !Directory.Exists(Path.Combine(dir.FullName, ".git")))
        {
            dir = dir.Parent;
        }

        var repoRoot = dir?.FullName ?? AppContext.BaseDirectory;
        return Path.Combine(repoRoot, "artifacts", "benchmarks");
    }

    /// <summary>
    /// Ensures the artifacts directory exists.
    /// </summary>
    public static void EnsureArtifactsDirectory()
    {
        Directory.CreateDirectory(ArtifactsPath);
    }
}
