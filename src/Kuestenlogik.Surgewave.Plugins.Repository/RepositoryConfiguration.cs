using System.Text.Json;
using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Plugins.Repository;

/// <summary>
/// Configuration for connector repositories.
/// Loaded from surgewave-repositories.json.
/// </summary>
public sealed class RepositoryConfiguration
{
    private const string DefaultConfigFileName = "surgewave-repositories.json";

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    /// <summary>
    /// List of configured repositories.
    /// </summary>
    public List<RepositoryEntry> Repositories { get; set; } = [];

    /// <summary>
    /// Default repository to use when none specified.
    /// </summary>
    public string? DefaultRepository { get; set; }

    /// <summary>
    /// Loads configuration from the default locations.
    /// Search order:
    /// 1. Current directory
    /// 2. User home directory (~/.surgewave/)
    /// 3. Global config (/etc/surgewave/ on Linux, %PROGRAMDATA%/surgewave/ on Windows)
    /// </summary>
    public static RepositoryConfiguration Load()
    {
        var searchPaths = GetSearchPaths();

        foreach (var path in searchPaths)
        {
            if (File.Exists(path))
            {
                try
                {
                    var json = File.ReadAllText(path);
                    var config = JsonSerializer.Deserialize<RepositoryConfiguration>(json, JsonOptions);
                    if (config != null)
                    {
                        return config;
                    }
                }
                catch (JsonException)
                {
                    // Invalid config file, continue searching
                }
            }
        }

        // Return default configuration
        return CreateDefault();
    }

    /// <summary>
    /// Loads configuration from a specific file.
    /// </summary>
    /// <param name="path">Path to the configuration file.</param>
    public static RepositoryConfiguration LoadFrom(string path)
    {
        if (!File.Exists(path))
        {
            throw new FileNotFoundException($"Configuration file not found: {path}");
        }

        var json = File.ReadAllText(path);
        return JsonSerializer.Deserialize<RepositoryConfiguration>(json, JsonOptions)
               ?? throw new InvalidOperationException("Failed to parse configuration");
    }

    /// <summary>
    /// Saves configuration to the user's home directory.
    /// </summary>
    public void Save()
    {
        var userConfigDir = GetUserConfigDirectory();
        Directory.CreateDirectory(userConfigDir);

        var configPath = Path.Combine(userConfigDir, DefaultConfigFileName);
        var json = JsonSerializer.Serialize(this, JsonOptions);
        File.WriteAllText(configPath, json);
    }

    /// <summary>
    /// Saves configuration to a specific file.
    /// </summary>
    /// <param name="path">Path to save to.</param>
    public void SaveTo(string path)
    {
        var directory = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }

        var json = JsonSerializer.Serialize(this, JsonOptions);
        File.WriteAllText(path, json);
    }

    /// <summary>
    /// Creates the default configuration with NuGet.org as the primary repository.
    /// </summary>
    public static RepositoryConfiguration CreateDefault()
    {
        return new RepositoryConfiguration
        {
            DefaultRepository = "nuget.org",
            Repositories =
            [
                new RepositoryEntry
                {
                    Name = "nuget.org",
                    Type = RepositoryType.NuGet,
                    Source = "https://api.nuget.org/v3/index.json",
                    Enabled = true
                },
                new RepositoryEntry
                {
                    Name = "surgewave-connectors",
                    Type = RepositoryType.Http,
                    Source = "https://kuestenlogik.github.io/Surgewave.Connectors",
                    Enabled = true,
                    PackagePrefix = "Kuestenlogik.Surgewave.Connector."
                }
            ]
        };
    }

    /// <summary>
    /// Adds a new repository to the configuration.
    /// </summary>
    public void AddRepository(RepositoryEntry entry)
    {
        // Remove existing with same name
        Repositories.RemoveAll(r => r.Name.Equals(entry.Name, StringComparison.OrdinalIgnoreCase));
        Repositories.Add(entry);
    }

    /// <summary>
    /// Removes a repository by name.
    /// </summary>
    public bool RemoveRepository(string name)
    {
        return Repositories.RemoveAll(r => r.Name.Equals(name, StringComparison.OrdinalIgnoreCase)) > 0;
    }

    /// <summary>
    /// Creates repository instances from the configuration.
    /// </summary>
    public IEnumerable<IConnectorRepository> CreateRepositories()
    {
        foreach (var entry in Repositories.Where(r => r.Enabled))
        {
            IConnectorRepository? repo = entry.Type switch
            {
                RepositoryType.NuGet => new NuGetConnectorRepository(
                    entry.Name,
                    entry.Source,
                    entry.PackagePrefix ?? "Kuestenlogik.Surgewave.Connector."),

                RepositoryType.Http => new HttpConnectorRepository(
                    entry.Name,
                    entry.Source,
                    entry.PackagePrefix ?? "Kuestenlogik.Surgewave.Connector."),

                RepositoryType.Marketplace => new SurgewaveMarketplaceRepository(
                    entry.Name,
                    entry.Source),

                _ => null
            };

            if (repo != null)
            {
                yield return repo;
            }
        }
    }

    private static IEnumerable<string> GetSearchPaths()
    {
        // Current directory
        yield return Path.Combine(Environment.CurrentDirectory, DefaultConfigFileName);

        // User config directory
        yield return Path.Combine(GetUserConfigDirectory(), DefaultConfigFileName);

        // Global config directory
        if (OperatingSystem.IsWindows())
        {
            var programData = Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData);
            yield return Path.Combine(programData, "surgewave", DefaultConfigFileName);
        }
        else
        {
            yield return Path.Combine("/etc", "surgewave", DefaultConfigFileName);
        }
    }

    private static string GetUserConfigDirectory()
    {
        var home = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        return Path.Combine(home, ".surgewave");
    }
}

/// <summary>
/// A repository entry in the configuration.
/// </summary>
public sealed class RepositoryEntry
{
    /// <summary>
    /// Unique name for this repository.
    /// </summary>
    public required string Name { get; set; }

    /// <summary>
    /// Repository type (NuGet, HTTP).
    /// </summary>
    public RepositoryType Type { get; set; } = RepositoryType.NuGet;

    /// <summary>
    /// Source URL of the repository.
    /// </summary>
    public required string Source { get; set; }

    /// <summary>
    /// Whether this repository is enabled.
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// Optional package ID prefix filter.
    /// </summary>
    public string? PackagePrefix { get; set; }

    /// <summary>
    /// Optional credentials for authenticated repositories.
    /// </summary>
    public RepositoryCredentials? Credentials { get; set; }
}

/// <summary>
/// Repository type.
/// </summary>
[JsonConverter(typeof(JsonStringEnumConverter))]
public enum RepositoryType
{
    /// <summary>
    /// NuGet package repository.
    /// </summary>
    NuGet,

    /// <summary>
    /// HTTP/REST API repository.
    /// </summary>
    Http,

    /// <summary>
    /// Surgewave Marketplace Server (self-hosted plugin registry).
    /// </summary>
    Marketplace
}

/// <summary>
/// Credentials for authenticated repository access.
/// </summary>
public sealed class RepositoryCredentials
{
    /// <summary>
    /// Username or API key name.
    /// </summary>
    public string? Username { get; set; }

    /// <summary>
    /// Password or API key value.
    /// </summary>
    public string? Password { get; set; }

    /// <summary>
    /// Bearer token for OAuth/JWT authentication.
    /// </summary>
    public string? Token { get; set; }
}
