using System.Text.Json;

namespace Kuestenlogik.Surgewave.Plugins.Sources;

/// <summary>
/// Plugin source that discovers and downloads Surgewave plugins from GitHub Releases.
/// Scans release assets for .swpkg (Surgewave Plugin Package) files.
/// </summary>
public sealed class GitHubPluginSource : IPluginSource, IDisposable
{
    private const string GitHubApiBase = "https://api.github.com";

    private readonly HttpClient _http;
    private readonly string _owner;
    private readonly string _repo;

    /// <summary>Display name for this source.</summary>
    public string Name { get; }

    /// <summary>Source type identifier.</summary>
    public string Type => "github";

    /// <summary>Repository slug (owner/repo) or full URL.</summary>
    public string Url { get; }

    /// <summary>
    /// Creates a new GitHub Releases plugin source.
    /// </summary>
    /// <param name="name">Display name.</param>
    /// <param name="repoSlug">Repository identifier — either "owner/repo" or a full GitHub URL.</param>
    public GitHubPluginSource(string name, string repoSlug)
    {
        Name = name;
        Url = repoSlug;

        (_owner, _repo) = ParseRepoSlug(repoSlug);

        _http = new HttpClient();
        _http.DefaultRequestHeaders.UserAgent.ParseAdd("Surgewave-CLI/1.0");
        _http.DefaultRequestHeaders.Accept.ParseAdd("application/vnd.github.v3+json");
    }

    /// <summary>
    /// Search for .swpkg assets across GitHub Releases.
    /// </summary>
    public async Task<IReadOnlyList<PluginPackageInfo>> SearchAsync(string? query, CancellationToken ct = default)
    {
        var releasesUrl = $"{GitHubApiBase}/repos/{_owner}/{_repo}/releases?per_page=30";
        var response = await _http.GetAsync(releasesUrl, ct);
        response.EnsureSuccessStatusCode();

        var json = await response.Content.ReadAsStringAsync(ct);
        using var doc = JsonDocument.Parse(json);

        var results = new List<PluginPackageInfo>();
        var lowerQuery = query?.ToLowerInvariant();

        foreach (var release in doc.RootElement.EnumerateArray())
        {
            var tagName = release.TryGetProperty("tag_name", out var tag) ? tag.GetString() ?? "" : "";
            var releaseName = release.TryGetProperty("name", out var rn) ? rn.GetString() ?? tagName : tagName;
            var body = release.TryGetProperty("body", out var b) ? b.GetString() : null;

            if (!release.TryGetProperty("assets", out var assets))
                continue;

            foreach (var asset in assets.EnumerateArray())
            {
                var assetName = asset.TryGetProperty("name", out var an) ? an.GetString() ?? "" : "";

                if (!assetName.EndsWith(".swpkg", StringComparison.OrdinalIgnoreCase))
                    continue;

                var pluginId = Path.GetFileNameWithoutExtension(assetName);

                // Apply query filter if provided
                if (!string.IsNullOrEmpty(lowerQuery) &&
                    !pluginId.Contains(lowerQuery, StringComparison.OrdinalIgnoreCase) &&
                    !releaseName.Contains(lowerQuery, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                results.Add(new PluginPackageInfo(
                    pluginId,
                    tagName.TrimStart('v'),
                    body?.Split('\n').FirstOrDefault()?.Trim(),
                    _owner,
                    Name));
            }
        }

        return results;
    }

    /// <summary>
    /// Downloads an .swpkg asset from a GitHub Release.
    /// </summary>
    public async Task<string> DownloadAsync(string pluginId, string? version, string targetDir, CancellationToken ct = default)
    {
        // Get the specific release or latest
        var releaseUrl = string.IsNullOrEmpty(version)
            ? $"{GitHubApiBase}/repos/{_owner}/{_repo}/releases/latest"
            : $"{GitHubApiBase}/repos/{_owner}/{_repo}/releases/tags/{(version.StartsWith('v') ? version : $"v{version}")}";

        var response = await _http.GetAsync(releaseUrl, ct);

        // If versioned tag with 'v' prefix fails, try without prefix
        if (!response.IsSuccessStatusCode && !string.IsNullOrEmpty(version) && !version.StartsWith('v'))
        {
            releaseUrl = $"{GitHubApiBase}/repos/{_owner}/{_repo}/releases/tags/{version}";
            response = await _http.GetAsync(releaseUrl, ct);
        }

        response.EnsureSuccessStatusCode();

        var json = await response.Content.ReadAsStringAsync(ct);
        using var doc = JsonDocument.Parse(json);

        var release = doc.RootElement;

        if (!release.TryGetProperty("assets", out var assets))
            throw new InvalidOperationException($"Release has no assets.");

        // Find the matching .swpkg asset
        string? downloadUrl = null;
        string? assetName = null;

        foreach (var asset in assets.EnumerateArray())
        {
            var name = asset.TryGetProperty("name", out var n) ? n.GetString() ?? "" : "";

            if (!name.EndsWith(".swpkg", StringComparison.OrdinalIgnoreCase))
                continue;

            var assetId = Path.GetFileNameWithoutExtension(name);

            if (assetId.Equals(pluginId, StringComparison.OrdinalIgnoreCase) ||
                name.Equals($"{pluginId}.swpkg", StringComparison.OrdinalIgnoreCase))
            {
                downloadUrl = asset.TryGetProperty("browser_download_url", out var url)
                    ? url.GetString()
                    : null;
                assetName = name;
                break;
            }
        }

        // If exact match not found, take first .swpkg
        if (downloadUrl == null)
        {
            foreach (var asset in assets.EnumerateArray())
            {
                var name = asset.TryGetProperty("name", out var n) ? n.GetString() ?? "" : "";

                if (name.EndsWith(".swpkg", StringComparison.OrdinalIgnoreCase))
                {
                    downloadUrl = asset.TryGetProperty("browser_download_url", out var url)
                        ? url.GetString()
                        : null;
                    assetName = name;
                    break;
                }
            }
        }

        if (string.IsNullOrEmpty(downloadUrl))
            throw new InvalidOperationException($"No .swpkg asset found for '{pluginId}' in release.");

        // Download the asset
        var bytes = await _http.GetByteArrayAsync(downloadUrl, ct);

        Directory.CreateDirectory(targetDir);
        var pluginPackagePath = Path.Combine(targetDir, assetName!);
        await File.WriteAllBytesAsync(pluginPackagePath, bytes, ct);

        return pluginPackagePath;
    }

    /// <summary>
    /// Releases the underlying HTTP client.
    /// </summary>
    public void Dispose() => _http.Dispose();

    private static (string owner, string repo) ParseRepoSlug(string slug)
    {
        // Handle full URLs like https://github.com/owner/repo
        if (slug.StartsWith("https://", StringComparison.OrdinalIgnoreCase) ||
            slug.StartsWith("http://", StringComparison.OrdinalIgnoreCase))
        {
            var uri = new Uri(slug);
            var segments = uri.AbsolutePath.Trim('/').Split('/');

            if (segments.Length >= 2)
                return (segments[0], segments[1].TrimEnd(".git".ToCharArray()));
        }

        // Handle owner/repo format
        var parts = slug.Split('/');
        if (parts.Length >= 2)
            return (parts[0], parts[1]);

        throw new ArgumentException($"Invalid GitHub repository slug: '{slug}'. Expected format: 'owner/repo'.");
    }
}
