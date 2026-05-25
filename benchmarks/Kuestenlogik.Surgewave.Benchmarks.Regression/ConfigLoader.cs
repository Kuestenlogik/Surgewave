using System.Text.Json;
using Kuestenlogik.Surgewave.Benchmarks.Regression.Models;

namespace Kuestenlogik.Surgewave.Benchmarks.Regression;

/// <summary>
/// Loads <see cref="RegressionConfig"/> from a JSON file.
/// </summary>
public static class ConfigLoader
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true
    };

    /// <summary>
    /// Loads a regression config from a JSON file.
    /// Returns default config if the path is null or the file does not exist.
    /// </summary>
    public static async Task<RegressionConfig> LoadAsync(string? configPath, CancellationToken ct = default)
    {
        if (string.IsNullOrEmpty(configPath) || !File.Exists(configPath))
        {
            return new RegressionConfig();
        }

        await using var stream = File.OpenRead(configPath);
        var config = await JsonSerializer.DeserializeAsync<RegressionConfig>(stream, JsonOptions, ct)
                         .ConfigureAwait(false);
        return config ?? new RegressionConfig();
    }
}
