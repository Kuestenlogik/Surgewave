using System.Text.Json;
using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Cli;

/// <summary>
/// Configuration file support for Surgewave CLI (~/.surgewave/config)
/// Provides default values that can be overridden by command-line options.
/// </summary>
public sealed class SurgewaveConfig
{
    private static readonly string ConfigDir = Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
        ".surgewave");

    private static readonly string ConfigPath = Path.Combine(ConfigDir, "config");

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    /// <summary>
    /// Default broker address (e.g., "localhost:9092")
    /// </summary>
    [JsonPropertyName("broker")]
    public string? BootstrapServer { get; set; }

    /// <summary>
    /// Default output format (table, json, plain)
    /// </summary>
    [JsonPropertyName("format")]
    public string? Format { get; set; }

    /// <summary>
    /// Enable verbose output by default
    /// </summary>
    [JsonPropertyName("verbose")]
    public bool? Verbose { get; set; }

    /// <summary>
    /// Default timeout in milliseconds
    /// </summary>
    [JsonPropertyName("timeout")]
    public int? Timeout { get; set; }

    /// <summary>
    /// Named profiles for different environments
    /// </summary>
    [JsonPropertyName("profiles")]
    public Dictionary<string, SurgewaveProfile>? Profiles { get; set; }

    /// <summary>
    /// Active profile name
    /// </summary>
    [JsonPropertyName("profile")]
    public string? ActiveProfile { get; set; }

    /// <summary>
    /// Loads the config from ~/.surgewave/config if it exists.
    /// Returns an empty config if the file doesn't exist.
    /// </summary>
    public static SurgewaveConfig Load()
    {
        if (!File.Exists(ConfigPath))
        {
            return new SurgewaveConfig();
        }

        try
        {
            var json = File.ReadAllText(ConfigPath);
            return JsonSerializer.Deserialize<SurgewaveConfig>(json, JsonOptions) ?? new SurgewaveConfig();
        }
        catch
        {
            // Invalid config file - return empty config
            return new SurgewaveConfig();
        }
    }

    /// <summary>
    /// Saves the config to ~/.surgewave/config
    /// </summary>
    public void Save()
    {
        Directory.CreateDirectory(ConfigDir);
        var json = JsonSerializer.Serialize(this, JsonOptions);
        File.WriteAllText(ConfigPath, json);
    }

    /// <summary>
    /// Gets the effective config values, applying active profile if set.
    /// </summary>
    public SurgewaveConfig GetEffective()
    {
        if (string.IsNullOrEmpty(ActiveProfile) || Profiles == null || !Profiles.TryGetValue(ActiveProfile, out var profile))
        {
            return this;
        }

        // Create merged config with profile values overriding base values
        return new SurgewaveConfig
        {
            BootstrapServer = profile.BootstrapServer ?? BootstrapServer,
            Format = profile.Format ?? Format,
            Verbose = profile.Verbose ?? Verbose,
            Timeout = profile.Timeout ?? Timeout,
            Profiles = Profiles,
            ActiveProfile = ActiveProfile
        };
    }

    /// <summary>
    /// Gets the bootstrap server value, checking environment variable first.
    /// Priority: CLI option > Surgewave_BOOTSTRAP_SERVER env > config file > default
    /// </summary>
    public static string GetBootstrapServer(string? cliValue)
    {
        // CLI option takes priority
        if (!string.IsNullOrEmpty(cliValue) && cliValue != "localhost:9092")
        {
            return cliValue;
        }

        // Check environment variable
        var envValue = Environment.GetEnvironmentVariable("Surgewave_BOOTSTRAP_SERVER");
        if (!string.IsNullOrEmpty(envValue))
        {
            return envValue;
        }

        // Check config file
        var config = Load().GetEffective();
        if (!string.IsNullOrEmpty(config.BootstrapServer))
        {
            return config.BootstrapServer;
        }

        // Return CLI value (or default)
        return cliValue ?? "localhost:9092";
    }

    /// <summary>
    /// Gets the output format from config if not specified on command line.
    /// </summary>
    public static OutputFormat GetFormat(OutputFormat cliValue)
    {
        if (cliValue != OutputFormat.Table)
        {
            return cliValue;
        }

        var config = Load().GetEffective();
        if (!string.IsNullOrEmpty(config.Format))
        {
            return Enum.TryParse<OutputFormat>(config.Format, ignoreCase: true, out var result)
                ? result
                : OutputFormat.Table;
        }

        return cliValue;
    }

    /// <summary>
    /// Gets the config directory path.
    /// </summary>
    public static string ConfigDirectory => ConfigDir;

    /// <summary>
    /// Gets the config file path.
    /// </summary>
    public static string ConfigFilePath => ConfigPath;

    /// <summary>
    /// Creates a default config file if one doesn't exist.
    /// </summary>
    public static void InitializeIfNotExists()
    {
        if (File.Exists(ConfigPath))
        {
            return;
        }

        var defaultConfig = new SurgewaveConfig
        {
            BootstrapServer = "localhost:9092",
            Format = "table",
            Verbose = false,
            Timeout = 30000,
            Profiles = new Dictionary<string, SurgewaveProfile>
            {
                ["local"] = new SurgewaveProfile
                {
                    BootstrapServer = "localhost:9092"
                },
                ["prod"] = new SurgewaveProfile
                {
                    BootstrapServer = "kafka.example.com:9092"
                }
            }
        };

        defaultConfig.Save();
    }
}

/// <summary>
/// Named profile for environment-specific settings
/// </summary>
public sealed class SurgewaveProfile
{
    [JsonPropertyName("broker")]
    public string? BootstrapServer { get; set; }

    [JsonPropertyName("format")]
    public string? Format { get; set; }

    [JsonPropertyName("verbose")]
    public bool? Verbose { get; set; }

    [JsonPropertyName("timeout")]
    public int? Timeout { get; set; }
}
