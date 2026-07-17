using System.Text.Json;
using System.Text.RegularExpressions;
using Kuestenlogik.Surgewave.Benchmarks.Regression.Models;

namespace Kuestenlogik.Surgewave.Benchmarks.Regression;

/// <summary>
/// Parses BenchmarkDotNet JSON export files into <see cref="BenchmarkEntry"/> dictionaries.
/// </summary>
public sealed partial class BenchmarkResultParser
{
    // BenchmarkDotNet writes raw NaN / Infinity tokens for NA benchmarks (failed, or filtered out
    // of a run). Those are not valid JSON, so System.Text.Json cannot parse a report containing
    // even one of them — which would crash the whole gate on a single flaky benchmark. Rewrite them
    // to null before parsing; the affected entries then read as zero and drop out of the comparison.
    [GeneratedRegex(@"(?<=:\s?)(-?Infinity|NaN)\b")]
    private static partial Regex NonFiniteTokenRegex();

    /// <summary>
    /// Parses a BenchmarkDotNet JSON report string into benchmark entries keyed by full name.
    /// </summary>
    public static Dictionary<string, BenchmarkEntry> ParseBdnReport(string jsonContent)
    {
        var results = new Dictionary<string, BenchmarkEntry>(StringComparer.Ordinal);

        var sanitized = NonFiniteTokenRegex().Replace(jsonContent, "null");
        using var doc = JsonDocument.Parse(sanitized);
        var root = doc.RootElement;

        if (!root.TryGetProperty("Benchmarks", out var benchmarksArray))
        {
            return results;
        }

        foreach (var benchmark in benchmarksArray.EnumerateArray())
        {
            var name = GetBenchmarkName(benchmark);
            if (string.IsNullOrEmpty(name))
            {
                continue;
            }

            var entry = new BenchmarkEntry();

            // Parse Statistics. For an NA benchmark this is null (JSON null), not an object, so
            // guard the kind — TryGetProperty on a null element throws.
            if (benchmark.TryGetProperty("Statistics", out var stats) && stats.ValueKind == JsonValueKind.Object)
            {
                entry.MeanNs = GetDouble(stats, "Mean");
                entry.MedianNs = GetDouble(stats, "Median");
                entry.StdDevNs = GetDouble(stats, "StandardDeviation")
                                 is 0.0 ? GetDouble(stats, "StdDev") : GetDouble(stats, "StandardDeviation");
            }

            // Parse Memory
            if (benchmark.TryGetProperty("Memory", out var memory) && memory.ValueKind == JsonValueKind.Object)
            {
                entry.AllocatedBytes = GetLong(memory, "BytesAllocatedPerOperation");
            }

            // Compute ops/sec from mean
            entry.OperationsPerSecond = entry.MeanNs > 0
                ? 1_000_000_000.0 / entry.MeanNs
                : 0;

            results[name] = entry;
        }

        return results;
    }

    /// <summary>
    /// Parses a BenchmarkDotNet JSON report file into benchmark entries.
    /// </summary>
    public static async Task<Dictionary<string, BenchmarkEntry>> ParseBdnReportFileAsync(
        string path, CancellationToken ct = default)
    {
        var json = await File.ReadAllTextAsync(path, ct).ConfigureAwait(false);
        return ParseBdnReport(json);
    }

    private static string? GetBenchmarkName(JsonElement benchmark)
    {
        // Prefer FullName, fall back to Type.Method
        if (benchmark.TryGetProperty("FullName", out var fullName))
        {
            return fullName.GetString();
        }

        string? type = null;
        string? method = null;

        if (benchmark.TryGetProperty("Type", out var typeElem))
        {
            type = typeElem.GetString();
        }

        if (benchmark.TryGetProperty("Method", out var methodElem))
        {
            method = methodElem.GetString();
        }

        if (type is not null && method is not null)
        {
            return $"{type}.{method}";
        }

        return method ?? type;
    }

    private static double GetDouble(JsonElement element, string propertyName)
    {
        if (element.TryGetProperty(propertyName, out var prop) && prop.ValueKind == JsonValueKind.Number)
        {
            return prop.GetDouble();
        }

        return 0.0;
    }

    private static long GetLong(JsonElement element, string propertyName)
    {
        if (element.TryGetProperty(propertyName, out var prop) && prop.ValueKind == JsonValueKind.Number)
        {
            return prop.GetInt64();
        }

        return 0;
    }
}
