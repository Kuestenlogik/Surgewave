using System.IO.Compression;
using System.Text.Json;

namespace Kuestenlogik.Surgewave.Plugins.Sources;

/// <summary>
/// Plugin source that queries NuGet v3 feeds to discover and download Surgewave plugins.
/// Uses HttpClient directly against the NuGet v3 API without requiring NuGet.Protocol packages.
/// </summary>
public sealed class NuGetPluginSource : IPluginSource, IDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = true };
    private readonly string? _apiKey;
    private readonly HttpClient _http;
    private string? _searchServiceUrl;
    private string? _packageBaseUrl;

    /// <summary>Display name for this source.</summary>
    public string Name { get; }

    /// <summary>Source type identifier.</summary>
    public string Type => "nuget";

    /// <summary>NuGet v3 service index URL.</summary>
    public string Url { get; }

    /// <summary>
    /// Creates a new NuGet plugin source.
    /// </summary>
    /// <param name="name">Display name.</param>
    /// <param name="url">NuGet v3 service index URL (e.g. https://api.nuget.org/v3/index.json).</param>
    /// <param name="apiKey">Optional API key for authenticated feeds.</param>
    public NuGetPluginSource(string name, string url, string? apiKey = null)
    {
        Name = name;
        Url = url.TrimEnd('/');
        _apiKey = apiKey;

        _http = new HttpClient();
        _http.DefaultRequestHeaders.UserAgent.ParseAdd("Surgewave-CLI/1.0");

        if (!string.IsNullOrEmpty(_apiKey))
        {
            _http.DefaultRequestHeaders.Add("X-NuGet-ApiKey", _apiKey);
        }
    }

    /// <summary>
    /// Search for Surgewave plugins on the NuGet feed.
    /// </summary>
    public async Task<IReadOnlyList<PluginPackageInfo>> SearchAsync(string? query, CancellationToken ct = default)
    {
        await EnsureServiceUrlsAsync(ct);

        var searchQuery = string.IsNullOrWhiteSpace(query) ? "Kuestenlogik.Surgewave" : query;
        var requestUrl = $"{_searchServiceUrl}?q={Uri.EscapeDataString(searchQuery)}&take=50&packageType=SurgewavePlugin";

        var response = await _http.GetAsync(requestUrl, ct);

        if (!response.IsSuccessStatusCode)
        {
            // Fall back without packageType filter
            requestUrl = $"{_searchServiceUrl}?q={Uri.EscapeDataString(searchQuery)}&take=50";
            response = await _http.GetAsync(requestUrl, ct);
            response.EnsureSuccessStatusCode();
        }

        var json = await response.Content.ReadAsStringAsync(ct);
        using var doc = JsonDocument.Parse(json);

        var results = new List<PluginPackageInfo>();

        if (!doc.RootElement.TryGetProperty("data", out var data))
            return results;

        foreach (var item in data.EnumerateArray())
        {
            var id = item.GetProperty("id").GetString() ?? "";
            var version = item.TryGetProperty("version", out var v) ? v.GetString() ?? "" : "";
            var description = item.TryGetProperty("description", out var d) ? d.GetString() : null;
            var authors = item.TryGetProperty("authors", out var a)
                ? (a.ValueKind == JsonValueKind.Array
                    ? string.Join(", ", a.EnumerateArray().Select(x => x.GetString()))
                    : a.GetString())
                : null;

            results.Add(new PluginPackageInfo(id, version, description, authors, Name));
        }

        return results;
    }

    /// <summary>
    /// Downloads a plugin from NuGet, extracts DLLs, and generates plugin.json.
    /// </summary>
    public async Task<string> DownloadAsync(string pluginId, string? version, string targetDir, CancellationToken ct = default)
    {
        await EnsureServiceUrlsAsync(ct);

        var lowerId = pluginId.ToLowerInvariant();

        // Resolve latest version if not specified
        if (string.IsNullOrEmpty(version))
        {
            version = await ResolveLatestVersionAsync(lowerId, ct);
        }

        var lowerVersion = version!.ToLowerInvariant();

        // Download .nupkg
        var nupkgUrl = $"{_packageBaseUrl}{lowerId}/{lowerVersion}/{lowerId}.{lowerVersion}.nupkg";
        var nupkgBytes = await _http.GetByteArrayAsync(nupkgUrl, ct);

        // Extract to plugin directory
        var pluginDir = Path.Combine(targetDir, pluginId);
        Directory.CreateDirectory(pluginDir);

        using var stream = new MemoryStream(nupkgBytes);
        using var archive = new ZipArchive(stream, ZipArchiveMode.Read);

        var extractedDlls = new List<string>();
        string? nuspecContent = null;

        foreach (var entry in archive.Entries)
        {
            // Extract DLLs from lib/net10.0/ (or fall back to net9.0, net8.0)
            if (IsTargetLibEntry(entry.FullName))
            {
                var fileName = Path.GetFileName(entry.FullName);
                var destPath = Path.Combine(pluginDir, fileName);

                entry.ExtractToFile(destPath, overwrite: true);
                extractedDlls.Add(fileName);
            }

            // Capture .nuspec for metadata
            if (entry.FullName.EndsWith(".nuspec", StringComparison.OrdinalIgnoreCase))
            {
                using var reader = new StreamReader(entry.Open());
                nuspecContent = await reader.ReadToEndAsync(ct);
            }
        }

        // If no net10.0 DLLs found, try broader extraction
        if (extractedDlls.Count == 0)
        {
            foreach (var entry in archive.Entries)
            {
                if (entry.FullName.StartsWith("lib/", StringComparison.OrdinalIgnoreCase) &&
                    entry.FullName.EndsWith(".dll", StringComparison.OrdinalIgnoreCase) &&
                    !string.IsNullOrEmpty(Path.GetFileName(entry.FullName)))
                {
                    var fileName = Path.GetFileName(entry.FullName);
                    var destPath = Path.Combine(pluginDir, fileName);

                    entry.ExtractToFile(destPath, overwrite: true);
                    extractedDlls.Add(fileName);
                    break; // Take first matching TFM only
                }
            }
        }

        // Generate plugin.json from nuspec metadata
        GeneratePluginManifest(pluginDir, pluginId, version!, nuspecContent, extractedDlls);

        return pluginDir;
    }

    /// <summary>
    /// Releases the underlying HTTP client.
    /// </summary>
    public void Dispose() => _http.Dispose();

    private async Task EnsureServiceUrlsAsync(CancellationToken ct)
    {
        if (_searchServiceUrl != null && _packageBaseUrl != null)
            return;

        var response = await _http.GetAsync(Url, ct);
        response.EnsureSuccessStatusCode();

        var json = await response.Content.ReadAsStringAsync(ct);
        using var doc = JsonDocument.Parse(json);

        var resources = doc.RootElement.GetProperty("resources");

        foreach (var resource in resources.EnumerateArray())
        {
            var type = resource.GetProperty("@type").GetString() ?? "";
            var id = resource.GetProperty("@id").GetString() ?? "";

            if (type.StartsWith("SearchQueryService", StringComparison.Ordinal) && _searchServiceUrl == null)
            {
                _searchServiceUrl = id;
            }
            else if (type.StartsWith("PackageBaseAddress", StringComparison.Ordinal) && _packageBaseUrl == null)
            {
                _packageBaseUrl = id.TrimEnd('/') + "/";
            }
        }

        _searchServiceUrl ??= Url.Replace("index.json", "query");
        _packageBaseUrl ??= Url.Replace("index.json", "flatcontainer/");
    }

    private async Task<string> ResolveLatestVersionAsync(string lowerId, CancellationToken ct)
    {
        var versionsUrl = $"{_packageBaseUrl}{lowerId}/index.json";
        var response = await _http.GetAsync(versionsUrl, ct);
        response.EnsureSuccessStatusCode();

        var json = await response.Content.ReadAsStringAsync(ct);
        using var doc = JsonDocument.Parse(json);

        var versions = doc.RootElement.GetProperty("versions");
        var lastVersion = "";

        foreach (var v in versions.EnumerateArray())
        {
            lastVersion = v.GetString() ?? lastVersion;
        }

        if (string.IsNullOrEmpty(lastVersion))
            throw new InvalidOperationException($"No versions found for package '{lowerId}'.");

        return lastVersion;
    }

    private static bool IsTargetLibEntry(string entryPath)
    {
        var normalized = entryPath.Replace('\\', '/');

        // Prefer net10.0, fall back to net9.0, net8.0
        return (normalized.StartsWith("lib/net10.0/", StringComparison.OrdinalIgnoreCase) ||
                normalized.StartsWith("lib/net9.0/", StringComparison.OrdinalIgnoreCase) ||
                normalized.StartsWith("lib/net8.0/", StringComparison.OrdinalIgnoreCase)) &&
               normalized.EndsWith(".dll", StringComparison.OrdinalIgnoreCase) &&
               !string.IsNullOrEmpty(Path.GetFileName(normalized));
    }

    private static void GeneratePluginManifest(
        string pluginDir, string pluginId, string version,
        string? nuspecContent, List<string> dlls)
    {
        var description = "";
        var authors = "";

        // Parse basic fields from nuspec XML (simple string-based extraction)
        if (!string.IsNullOrEmpty(nuspecContent))
        {
            description = ExtractXmlElement(nuspecContent, "description") ?? "";
            authors = ExtractXmlElement(nuspecContent, "authors") ?? "";
        }

        var mainDll = dlls.FirstOrDefault(d =>
            d.StartsWith(pluginId, StringComparison.OrdinalIgnoreCase)) ?? dlls.FirstOrDefault() ?? "";

        var manifest = new
        {
            name = pluginId,
            version,
            description,
            authors,
            entryPoint = mainDll,
            source = "nuget"
        };

        var json = JsonSerializer.Serialize(manifest, JsonOptions);
        File.WriteAllText(Path.Combine(pluginDir, "plugin.json"), json);
    }

    private static string? ExtractXmlElement(string xml, string element)
    {
        var startTag = $"<{element}>";
        var endTag = $"</{element}>";

        var start = xml.IndexOf(startTag, StringComparison.OrdinalIgnoreCase);
        if (start < 0) return null;

        start += startTag.Length;
        var end = xml.IndexOf(endTag, start, StringComparison.OrdinalIgnoreCase);
        if (end < 0) return null;

        return xml[start..end].Trim();
    }
}
