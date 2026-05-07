namespace Kuestenlogik.Surgewave.Plugins.Sources;

/// <summary>
/// A remote source for discovering and downloading Surgewave plugins.
/// Implementations include NuGet feeds, HTTP/Marketplace endpoints, and GitHub Releases.
/// </summary>
public interface IPluginSource
{
    /// <summary>Display name for this source.</summary>
    string Name { get; }

    /// <summary>Source type identifier (nuget, http, github).</summary>
    string Type { get; }

    /// <summary>Base URL or identifier for this source.</summary>
    string Url { get; }

    /// <summary>
    /// Search for plugins matching the query.
    /// </summary>
    Task<IReadOnlyList<PluginPackageInfo>> SearchAsync(string? query, CancellationToken ct = default);

    /// <summary>
    /// Downloads a plugin to a temporary directory and returns the path to the .swpkg file or extracted directory.
    /// </summary>
    Task<string> DownloadAsync(string pluginId, string? version, string targetDir, CancellationToken ct = default);
}

/// <summary>
/// Information about a plugin available from a remote source.
/// </summary>
public sealed record PluginPackageInfo(
    string Id,
    string Version,
    string? Description,
    string? Authors,
    string Source);
