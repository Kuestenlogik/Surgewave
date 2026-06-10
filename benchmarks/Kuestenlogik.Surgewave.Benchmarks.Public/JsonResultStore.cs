using System.Text.Json;
using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Benchmarks.Public;

/// <summary>
/// JSON serialisation for <see cref="PublicBenchmarkReport"/> — what
/// gets committed under <c>benchmarks/baselines/</c> as the
/// reference for <c>--compare</c>. Pretty-printed for diff-friendly
/// review, camelCase to match the rest of Surgewave's JSON surface
/// (REST APIs + Control-UI HTTP-clients).
/// </summary>
public static class JsonResultStore
{
    private static readonly JsonSerializerOptions Options = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DictionaryKeyPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    public static string Serialize(PublicBenchmarkReport report) =>
        JsonSerializer.Serialize(report, Options);

    public static PublicBenchmarkReport Deserialize(string json) =>
        JsonSerializer.Deserialize<PublicBenchmarkReport>(json, Options)
        ?? throw new InvalidOperationException("Failed to deserialize PublicBenchmarkReport — JSON was null.");

    public static PublicBenchmarkReport LoadFromFile(string path) =>
        Deserialize(File.ReadAllText(path));

    public static void SaveToFile(PublicBenchmarkReport report, string path) =>
        File.WriteAllText(path, Serialize(report));
}
