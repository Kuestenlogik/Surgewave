using System.Text.Json;
using Kuestenlogik.Surgewave.Benchmarks.Regression.Models;

namespace Kuestenlogik.Surgewave.Benchmarks.Regression;

/// <summary>
/// Manages loading, saving, and merging benchmark baseline files.
/// </summary>
public sealed class BaselineManager
{
    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true
    };

    /// <summary>
    /// Loads a benchmark baseline from a JSON file.
    /// Returns an empty baseline if the file does not exist.
    /// </summary>
    public static async Task<BenchmarkBaseline> LoadAsync(string path, CancellationToken ct = default)
    {
        if (!File.Exists(path))
        {
            return new BenchmarkBaseline();
        }

        await using var stream = File.OpenRead(path);
        var baseline = await JsonSerializer.DeserializeAsync<BenchmarkBaseline>(stream, SerializerOptions, ct)
                       .ConfigureAwait(false);

        return baseline ?? new BenchmarkBaseline();
    }

    /// <summary>
    /// Saves a benchmark baseline to a JSON file, creating directories as needed.
    /// </summary>
    public static async Task SaveAsync(string path, BenchmarkBaseline baseline, CancellationToken ct = default)
    {
        var directory = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }

        await using var stream = File.Create(path);
        await JsonSerializer.SerializeAsync(stream, baseline, SerializerOptions, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Merges new benchmark results into an existing baseline.
    /// Existing entries are updated; new entries are added; entries not in the new results are preserved.
    /// </summary>
    public static BenchmarkBaseline MergeResults(BenchmarkBaseline existing, Dictionary<string, BenchmarkEntry> newResults)
    {
        var merged = new BenchmarkBaseline
        {
            Version = existing.Version,
            Timestamp = DateTimeOffset.UtcNow,
            Environment = existing.Environment,
            Benchmarks = new Dictionary<string, BenchmarkEntry>(existing.Benchmarks, StringComparer.Ordinal)
        };

        foreach (var (name, entry) in newResults)
        {
            merged.Benchmarks[name] = entry;
        }

        return merged;
    }
}
