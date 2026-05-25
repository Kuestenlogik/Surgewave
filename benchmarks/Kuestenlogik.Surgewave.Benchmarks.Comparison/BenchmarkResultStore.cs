using System.Text.Json;
using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Benchmarks.Comparison;

/// <summary>
/// A single benchmark result entry persisted as JSON.
/// </summary>
public sealed record BenchmarkResultEntry(
    [property: JsonPropertyName("platform")]    string Platform,
    [property: JsonPropertyName("timestamp")]   DateTime Timestamp,
    [property: JsonPropertyName("messageCount")] int MessageCount,
    [property: JsonPropertyName("messageSize")]  int MessageSize,
    [property: JsonPropertyName("producerMsgPerSec")] double ProducerMsgPerSec,
    [property: JsonPropertyName("producerMBPerSec")]  double ProducerMBPerSec,
    [property: JsonPropertyName("consumerMsgPerSec")] double ConsumerMsgPerSec,
    [property: JsonPropertyName("consumerMBPerSec")]  double ConsumerMBPerSec);

/// <summary>
/// Saves and loads benchmark results from <c>artifacts/benchmarks/results/</c>.
/// Each platform is stored in its own JSON file so runs can be done independently
/// and then compared with the <c>compare</c> command.
/// </summary>
public static class BenchmarkResultStore
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    /// <summary>Returns the results directory, creating it if needed.</summary>
    public static string ResultsDirectory
    {
        get
        {
            // Walk up from the executing assembly location to find the repo root, then
            // resolve artifacts/benchmarks/results.  Fall back to CWD-relative path.
            var dir = Path.Combine(GetRepoRoot(), "artifacts", "benchmarks", "results");
            Directory.CreateDirectory(dir);
            return dir;
        }
    }

    /// <summary>Saves <paramref name="entry"/> to <c>results/{platform}.json</c>.</summary>
    public static void Save(BenchmarkResultEntry entry)
    {
        var path = Path.Combine(ResultsDirectory, $"{entry.Platform}.json");
        var json = JsonSerializer.Serialize(entry, JsonOptions);
        File.WriteAllText(path, json);
        Console.WriteLine($"  Results saved: {path}");
    }

    /// <summary>Loads a previously saved result for <paramref name="platform"/>.</summary>
    public static BenchmarkResultEntry? Load(string platform)
    {
        var path = Path.Combine(ResultsDirectory, $"{platform}.json");
        if (!File.Exists(path))
            return null;

        var json = File.ReadAllText(path);
        return JsonSerializer.Deserialize<BenchmarkResultEntry>(json, JsonOptions);
    }

    /// <summary>Loads all saved result files from the results directory.</summary>
    public static IReadOnlyList<BenchmarkResultEntry> LoadAll()
    {
        var dir = ResultsDirectory;
        var results = new List<BenchmarkResultEntry>();

        foreach (var file in Directory.EnumerateFiles(dir, "*.json").OrderBy(f => f))
        {
            try
            {
                var json = File.ReadAllText(file);
                var entry = JsonSerializer.Deserialize<BenchmarkResultEntry>(json, JsonOptions);
                if (entry is not null)
                    results.Add(entry);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  Warning: could not load {file}: {ex.Message}");
            }
        }

        return results;
    }

    /// <summary>
    /// Prints a formatted comparison table sorted by producer throughput descending.
    /// Surgewave (surgewave-kafka, surgewave-native) is used as the baseline when present;
    /// otherwise the fastest entry is the baseline.
    /// </summary>
    public static void PrintComparisonTable(IReadOnlyList<BenchmarkResultEntry> results)
    {
        if (results.Count == 0)
        {
            Console.WriteLine("  No results to compare. Run benchmark commands first.");
            return;
        }

        // Sort by producer msg/sec descending
        var sorted = results.OrderByDescending(r => r.ProducerMsgPerSec).ToList();

        // Prefer surgewave-native as baseline; fall back to surgewave-kafka; then to fastest
        var baseline =
            sorted.FirstOrDefault(r => r.Platform == "surgewave-native") ??
            sorted.FirstOrDefault(r => r.Platform == "surgewave-kafka") ??
            sorted[0];

        var colW = 16;
        var labelW = 24;

        Console.WriteLine();
        Console.WriteLine("╔══════════════════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║                      CROSS-PLATFORM COMPARISON RESULTS                       ║");
        Console.WriteLine("╚══════════════════════════════════════════════════════════════════════════════╝");
        Console.WriteLine($"  Baseline: {baseline.Platform}  |  Results sorted by producer throughput");
        Console.WriteLine();

        // Header
        var header = "Platform".PadRight(labelW) + " " +
                     "Prod msg/s".PadRight(colW) + " " +
                     "Prod MB/s".PadRight(colW) + " " +
                     "Cons msg/s".PadRight(colW) + " " +
                     "Cons MB/s".PadRight(colW) + " " +
                     "vs baseline";
        Console.WriteLine(header);
        Console.WriteLine(new string('─', header.Length));

        foreach (var r in sorted)
        {
            var delta = baseline.ProducerMsgPerSec > 0
                ? (r.ProducerMsgPerSec - baseline.ProducerMsgPerSec) / baseline.ProducerMsgPerSec * 100
                : 0;

            var deltaStr = r.Platform == baseline.Platform
                ? "(baseline)"
                : $"{(delta >= 0 ? "+" : "")}{delta:N1}%";

            Console.WriteLine(
                r.Platform.PadRight(labelW) + " " +
                r.ProducerMsgPerSec.ToString("N0").PadRight(colW) + " " +
                r.ProducerMBPerSec.ToString("N1").PadRight(colW) + " " +
                r.ConsumerMsgPerSec.ToString("N0").PadRight(colW) + " " +
                r.ConsumerMBPerSec.ToString("N1").PadRight(colW) + " " +
                deltaStr);
        }

        Console.WriteLine();
        Console.WriteLine($"  Results directory: {ResultsDirectory}");
        Console.WriteLine($"  Timestamps: {string.Join(", ", sorted.Select(r => $"{r.Platform}={r.Timestamp:yyyy-MM-dd HH:mm}"))}");
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static string GetRepoRoot()
    {
        // Heuristic: walk up from the assembly directory until we find a .slnx or .sln file.
        var dir = Path.GetDirectoryName(typeof(BenchmarkResultStore).Assembly.Location) ?? Directory.GetCurrentDirectory();
        var candidate = dir;
        while (!string.IsNullOrEmpty(candidate))
        {
            if (Directory.EnumerateFiles(candidate, "*.slnx").Any() ||
                Directory.EnumerateFiles(candidate, "*.sln").Any())
            {
                return candidate;
            }
            candidate = Path.GetDirectoryName(candidate);
        }
        // Fall back to CWD
        return Directory.GetCurrentDirectory();
    }
}
