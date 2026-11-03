using System.Text.Json;
using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Plugins.Sources;

/// <summary>
/// Configuration for plugin sources. Stored in ~/.surgewave/plugin-sources.json.
/// </summary>
public sealed class PluginSourceConfig
{
    private static readonly string ConfigDir = Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), ".surgewave");
    private static readonly string ConfigPath = Path.Combine(ConfigDir, "plugin-sources.json");

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        PropertyNameCaseInsensitive = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    [JsonPropertyName("sources")]
    public List<SourceEntry> Sources { get; set; } = [];

    /// <summary>
    /// Load configuration from ~/.surgewave/plugin-sources.json.
    /// Returns default config if file doesn't exist.
    /// </summary>
    public static PluginSourceConfig Load()
    {
        if (!File.Exists(ConfigPath))
            return CreateDefault();

        try
        {
            var json = File.ReadAllText(ConfigPath);
            return JsonSerializer.Deserialize<PluginSourceConfig>(json, JsonOptions) ?? CreateDefault();
        }
        catch
        {
            return CreateDefault();
        }
    }

    /// <summary>
    /// Save configuration to ~/.surgewave/plugin-sources.json.
    /// </summary>
    public void Save()
    {
        Directory.CreateDirectory(ConfigDir);
        var json = JsonSerializer.Serialize(this, JsonOptions);
        File.WriteAllText(ConfigPath, json);
    }

    /// <summary>The sources a fresh install knows, before the user adds or removes any.</summary>
    public static PluginSourceConfig CreateDefault() => new()
    {
        Sources =
        [
            new SourceEntry { Name = "nuget", Type = "nuget", Url = "https://api.nuget.org/v3/index.json" },
            new SourceEntry { Name = "marketplace", Type = "http", Url = "http://localhost:5060" },
            // Küstenlogik's own plugins ship as .swpkg assets on their repository's GitHub releases (#137).
            new SourceEntry { Name = "surgewave-ai", Type = "github", Url = "Kuestenlogik/Surgewave.Ai" }
        ]
    };

    public sealed class SourceEntry
    {
        [JsonPropertyName("name")]
        public required string Name { get; set; }

        [JsonPropertyName("type")]
        public required string Type { get; set; }

        [JsonPropertyName("url")]
        public required string Url { get; set; }

        [JsonPropertyName("apiKey")]
        public string? ApiKey { get; set; }
    }
}
