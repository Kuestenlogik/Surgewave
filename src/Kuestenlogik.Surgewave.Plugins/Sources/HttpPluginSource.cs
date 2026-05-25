using System.Text.Json;

namespace Kuestenlogik.Surgewave.Plugins.Sources;

/// <summary>
/// Plugin source that queries a Marketplace-style HTTP API for discovering and downloading Surgewave plugins.
/// </summary>
public sealed class HttpPluginSource : IPluginSource, IDisposable
{
    private readonly HttpClient _http;

    /// <summary>Display name for this source.</summary>
    public string Name { get; }

    /// <summary>Source type identifier.</summary>
    public string Type => "http";

    /// <summary>Base URL for the marketplace API.</summary>
    public string Url { get; }

    /// <summary>
    /// Creates a new HTTP marketplace plugin source.
    /// </summary>
    /// <param name="name">Display name.</param>
    /// <param name="url">Base URL of the marketplace API.</param>
    public HttpPluginSource(string name, string url)
    {
        Name = name;
        Url = url.TrimEnd('/');

        _http = new HttpClient();
        _http.DefaultRequestHeaders.UserAgent.ParseAdd("Surgewave-CLI/1.0");
        _http.DefaultRequestHeaders.Accept.ParseAdd("application/json");
    }

    /// <summary>
    /// Search for plugins on the HTTP marketplace.
    /// </summary>
    public async Task<IReadOnlyList<PluginPackageInfo>> SearchAsync(string? query, CancellationToken ct = default)
    {
        var requestUrl = string.IsNullOrWhiteSpace(query)
            ? $"{Url}/api/plugins"
            : $"{Url}/api/plugins?q={Uri.EscapeDataString(query)}";

        var response = await _http.GetAsync(requestUrl, ct);
        response.EnsureSuccessStatusCode();

        var json = await response.Content.ReadAsStringAsync(ct);
        using var doc = JsonDocument.Parse(json);

        var results = new List<PluginPackageInfo>();
        var root = doc.RootElement;

        // Support both array and object-with-data responses
        var items = root.ValueKind == JsonValueKind.Array
            ? root
            : root.TryGetProperty("data", out var data) ? data : root;

        if (items.ValueKind != JsonValueKind.Array)
            return results;

        foreach (var item in items.EnumerateArray())
        {
            var id = GetStringProp(item, "id") ?? GetStringProp(item, "pluginId") ?? "";
            var version = GetStringProp(item, "version") ?? "";
            var description = GetStringProp(item, "description");
            var authors = GetStringProp(item, "authors") ?? GetStringProp(item, "author");

            if (!string.IsNullOrEmpty(id))
            {
                results.Add(new PluginPackageInfo(id, version, description, authors, Name));
            }
        }

        return results;
    }

    /// <summary>
    /// Downloads a plugin .swpkg package from the HTTP marketplace.
    /// </summary>
    public async Task<string> DownloadAsync(string pluginId, string? version, string targetDir, CancellationToken ct = default)
    {
        version ??= "latest";
        var downloadUrl = $"{Url}/api/plugins/{Uri.EscapeDataString(pluginId)}/{Uri.EscapeDataString(version)}/download";

        var response = await _http.GetAsync(downloadUrl, ct);
        response.EnsureSuccessStatusCode();

        Directory.CreateDirectory(targetDir);

        var pluginPackagePath = Path.Combine(targetDir, $"{pluginId}.swpkg");
        var bytes = await response.Content.ReadAsByteArrayAsync(ct);
        await File.WriteAllBytesAsync(pluginPackagePath, bytes, ct);

        return pluginPackagePath;
    }

    /// <summary>
    /// Releases the underlying HTTP client.
    /// </summary>
    public void Dispose() => _http.Dispose();

    private static string? GetStringProp(JsonElement element, string propertyName)
    {
        return element.TryGetProperty(propertyName, out var prop) && prop.ValueKind == JsonValueKind.String
            ? prop.GetString()
            : null;
    }
}
