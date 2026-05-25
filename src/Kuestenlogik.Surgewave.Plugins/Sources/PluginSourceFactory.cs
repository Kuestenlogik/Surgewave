namespace Kuestenlogik.Surgewave.Plugins.Sources;

/// <summary>
/// Factory for creating <see cref="IPluginSource"/> instances from configuration entries.
/// </summary>
public static class PluginSourceFactory
{
    /// <summary>
    /// Creates an <see cref="IPluginSource"/> from a configuration entry.
    /// </summary>
    /// <param name="entry">The source configuration entry.</param>
    /// <returns>A new plugin source instance.</returns>
    /// <exception cref="ArgumentException">Thrown when the source type is unknown.</exception>
    public static IPluginSource Create(PluginSourceConfig.SourceEntry entry) => entry.Type.ToLowerInvariant() switch
    {
        "nuget" => new NuGetPluginSource(entry.Name, entry.Url, entry.ApiKey),
        "http" => new HttpPluginSource(entry.Name, entry.Url),
        "github" => new GitHubPluginSource(entry.Name, entry.Url),
        _ => throw new ArgumentException($"Unknown plugin source type: '{entry.Type}'. Supported types: nuget, http, github.")
    };

    /// <summary>
    /// Creates all plugin sources from the given configuration.
    /// </summary>
    /// <param name="config">The plugin source configuration.</param>
    /// <returns>A list of plugin source instances.</returns>
    public static IReadOnlyList<IPluginSource> CreateAll(PluginSourceConfig config)
        => config.Sources.Select(Create).ToList();
}
