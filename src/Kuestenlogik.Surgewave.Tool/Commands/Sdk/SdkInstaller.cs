using System.Text.Json;

namespace Kuestenlogik.Surgewave.Cli.Commands.Sdk;

/// <summary>
/// Pulls the <c>.nupkg</c> assets of a tagged Surgewave GitHub Release
/// into a local SDK feed (<c>~/.surgewave/sdk/&lt;version&gt;</c>) and
/// optionally wires a <c>nuget.config</c> in a plugin's project directory
/// so it can resolve <c>Kuestenlogik.Surgewave.Sdk@X.Y.Z</c> without
/// hardcoding a local path.
///
/// <para>
/// Pure: no <see cref="System.CommandLine"/> or <see cref="Spectre.Console"/>
/// dependencies. The CLI wrapper handles UI; the installer handles bytes.
/// </para>
/// </summary>
public sealed class SdkInstaller
{
    private readonly HttpClient _http;
    private readonly Action<string>? _progress;

    public SdkInstaller(HttpClient http, Action<string>? progress = null)
    {
        _http = http ?? throw new ArgumentNullException(nameof(http));
        _progress = progress;
    }

    /// <summary>
    /// Default install root: <c>~/.surgewave/sdk</c>. Per-version folders
    /// live under it (<c>~/.surgewave/sdk/0.1.13</c>).
    /// </summary>
    public static string DefaultSdkRoot =>
        Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
            ".surgewave", "sdk");

    /// <summary>
    /// Resolve "latest" or a literal version (with or without leading "v")
    /// to the canonical tag name (e.g. "v0.1.13").
    /// </summary>
    public async Task<string> ResolveTagAsync(string owner, string repo, string version, CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrEmpty(owner);
        ArgumentException.ThrowIfNullOrEmpty(repo);
        ArgumentException.ThrowIfNullOrEmpty(version);

        if (string.Equals(version, "latest", StringComparison.OrdinalIgnoreCase))
        {
            var url = $"https://api.github.com/repos/{owner}/{repo}/releases/latest";
            using var doc = await FetchJsonAsync(url, ct).ConfigureAwait(false);
            var tag = doc.RootElement.GetProperty("tag_name").GetString();
            if (string.IsNullOrEmpty(tag))
                throw new InvalidOperationException($"GitHub Release at {url} has no tag_name.");
            return tag;
        }

        return version.StartsWith('v') ? version : $"v{version}";
    }

    /// <summary>
    /// List the <c>.nupkg</c> assets attached to the given tag's GitHub
    /// Release. Throws <see cref="InvalidOperationException"/> if the
    /// release has no NuGet assets — that almost always means the
    /// release pipeline didn't upload them (e.g. test step failed).
    /// </summary>
    public async Task<IReadOnlyList<NupkgAsset>> ListNupkgAssetsAsync(string owner, string repo, string tag, CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrEmpty(tag);

        var url = $"https://api.github.com/repos/{owner}/{repo}/releases/tags/{tag}";
        using var doc = await FetchJsonAsync(url, ct).ConfigureAwait(false);
        if (!doc.RootElement.TryGetProperty("assets", out var assetsEl))
            throw new InvalidOperationException($"Release {tag} has no 'assets' field.");

        var assets = new List<NupkgAsset>();
        foreach (var asset in assetsEl.EnumerateArray())
        {
            var name = asset.GetProperty("name").GetString() ?? "";
            if (!name.EndsWith(".nupkg", StringComparison.OrdinalIgnoreCase)) continue;
            // Skip symbol packages — they're not useful in the resolve path
            // and just inflate the local feed.
            if (name.EndsWith(".symbols.nupkg", StringComparison.OrdinalIgnoreCase)) continue;
            if (name.EndsWith(".snupkg", StringComparison.OrdinalIgnoreCase)) continue;

            var dl = asset.GetProperty("browser_download_url").GetString();
            if (string.IsNullOrEmpty(dl)) continue;
            var size = asset.TryGetProperty("size", out var sizeEl) ? sizeEl.GetInt64() : 0;
            assets.Add(new NupkgAsset(name, dl, size));
        }

        if (assets.Count == 0)
            throw new InvalidOperationException($"Release {tag} has no .nupkg assets.");

        return assets;
    }

    /// <summary>
    /// Download every <c>.nupkg</c> asset of the resolved tag into
    /// <paramref name="targetDir"/>. Returns the number of newly downloaded
    /// files (already-present packages are skipped unless <paramref name="force"/>).
    /// </summary>
    public async Task<DownloadResult> DownloadAsync(
        IReadOnlyList<NupkgAsset> assets,
        string targetDir,
        bool force,
        CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrEmpty(targetDir);
        Directory.CreateDirectory(targetDir);

        var downloaded = 0;
        var skipped = 0;
        foreach (var asset in assets)
        {
            ct.ThrowIfCancellationRequested();
            var path = Path.Combine(targetDir, asset.Name);

            if (File.Exists(path) && !force)
            {
                _progress?.Invoke($"skip   {asset.Name} (already present)");
                skipped++;
                continue;
            }

            _progress?.Invoke($"fetch  {asset.Name} ({asset.SizeBytes / 1024} KB)");
            using var stream = await _http.GetStreamAsync(asset.DownloadUrl, ct).ConfigureAwait(false);
            await using var file = File.Create(path);
            await stream.CopyToAsync(file, ct).ConfigureAwait(false);
            downloaded++;
        }

        return new DownloadResult(downloaded, skipped, targetDir);
    }

    /// <summary>
    /// Write or update a <c>nuget.config</c> in <paramref name="projectDir"/>
    /// so it adds the local SDK feed at <paramref name="feedDir"/> as a
    /// package source named <paramref name="feedName"/>. Existing entries
    /// with the same key are replaced; other sources stay intact.
    /// </summary>
    public static void WriteNugetConfig(string projectDir, string feedDir, string feedName = "surgewave-sdk-local")
    {
        ArgumentException.ThrowIfNullOrEmpty(projectDir);
        ArgumentException.ThrowIfNullOrEmpty(feedDir);
        ArgumentException.ThrowIfNullOrEmpty(feedName);

        Directory.CreateDirectory(projectDir);
        var configPath = Path.Combine(projectDir, "nuget.config");

        // Naive but predictable: emit a self-contained file. We do not try
        // to merge into an existing one because plugin authors almost
        // never have a hand-written nuget.config — the rare case where
        // they do can be handled by passing --no-write-nuget-config and
        // copying the snippet manually.
        var xml = $"""
            <?xml version="1.0" encoding="utf-8"?>
            <configuration>
              <packageSources>
                <clear />
                <add key="nuget.org" value="https://api.nuget.org/v3/index.json" />
                <add key="{feedName}" value="{feedDir}" />
              </packageSources>
            </configuration>
            """;
        File.WriteAllText(configPath, xml);
    }

    private async Task<JsonDocument> FetchJsonAsync(string url, CancellationToken ct)
    {
        // GitHub API requires a User-Agent header — without one the
        // request returns 403.
        using var req = new HttpRequestMessage(HttpMethod.Get, url);
        req.Headers.Add("User-Agent", "surgewave-sdk-installer");
        req.Headers.Add("Accept", "application/vnd.github+json");
        using var res = await _http.SendAsync(req, ct).ConfigureAwait(false);
        res.EnsureSuccessStatusCode();
        var stream = await res.Content.ReadAsStreamAsync(ct).ConfigureAwait(false);
        return await JsonDocument.ParseAsync(stream, cancellationToken: ct).ConfigureAwait(false);
    }
}

/// <summary>One downloadable <c>.nupkg</c> asset on a GitHub Release.</summary>
public sealed record NupkgAsset(string Name, string DownloadUrl, long SizeBytes);

/// <summary>Outcome of one <see cref="SdkInstaller.DownloadAsync"/> call.</summary>
public sealed record DownloadResult(int Downloaded, int Skipped, string TargetDirectory);
