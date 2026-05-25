using System.Diagnostics.CodeAnalysis;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Plugins.Repository;

/// <summary>
/// Connector repository backed by HTTP/REST API or static index file.
/// Supports two modes:
/// 1. Static index: GET {baseUrl}/packages.json returns all packages
/// 2. REST API: GET {baseUrl}/api/packages?q={query}&amp;skip={skip}&amp;take={take}
/// </summary>
[SuppressMessage("Design", "CA1054:URI parameters should not be strings", Justification = "URLs are dynamically constructed")]
[SuppressMessage("Design", "CA2234:Pass System.Uri objects instead of strings", Justification = "URLs are dynamically constructed")]
public sealed class HttpConnectorRepository : IConnectorRepository, IDisposable
{
    private readonly HttpClient _httpClient;
    private readonly bool _ownsHttpClient;
    private readonly string _packagePrefix;
    private readonly RepositoryMode _mode;
    private List<ConnectorPackageInfo>? _cachedPackages;
    private DateTime _cacheExpiry = DateTime.MinValue;
    private static readonly TimeSpan CacheDuration = TimeSpan.FromMinutes(5);

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    /// <summary>
    /// Creates a new HTTP connector repository.
    /// </summary>
    /// <param name="name">Repository name.</param>
    /// <param name="source">Base URL of the repository.</param>
    /// <param name="packagePrefix">Package ID prefix filter.</param>
    /// <param name="httpClient">Optional HTTP client to use.</param>
    public HttpConnectorRepository(
        string name,
        string source,
        string packagePrefix = "Kuestenlogik.Surgewave.Connector.",
        HttpClient? httpClient = null)
    {
        Name = name;
        Source = source.TrimEnd('/');
        _packagePrefix = packagePrefix;
        _ownsHttpClient = httpClient == null;
        _httpClient = httpClient ?? new HttpClient();
        _mode = DetectMode(Source);
    }

    /// <inheritdoc />
    public string Name { get; }

    /// <inheritdoc />
    public string Source { get; }

    /// <inheritdoc />
    public async Task<IReadOnlyList<ConnectorPackageInfo>> SearchAsync(
        string? query,
        int skip = 0,
        int take = 20,
        CancellationToken cancellationToken = default)
    {
        if (_mode == RepositoryMode.StaticIndex)
        {
            return await SearchStaticIndexAsync(query, skip, take, cancellationToken);
        }

        return await SearchRestApiAsync(query, skip, take, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<ConnectorPackageInfo?> GetPackageAsync(
        string packageId,
        string? version = null,
        CancellationToken cancellationToken = default)
    {
        if (_mode == RepositoryMode.StaticIndex)
        {
            var packages = await GetAllPackagesAsync(cancellationToken);
            var package = packages.FirstOrDefault(p =>
                p.PackageId.Equals(packageId, StringComparison.OrdinalIgnoreCase));

            if (package == null) return null;

            // If specific version requested, check if it's available
            if (!string.IsNullOrWhiteSpace(version) &&
                !package.AvailableVersions.Contains(version, StringComparer.OrdinalIgnoreCase))
            {
                return null;
            }

            return package;
        }

        // REST API mode
        var url = $"{Source}/api/packages/{Uri.EscapeDataString(packageId)}";
        if (!string.IsNullOrWhiteSpace(version))
        {
            url += $"?version={Uri.EscapeDataString(version)}";
        }

        try
        {
            var response = await _httpClient.GetAsync(url, cancellationToken);
            if (!response.IsSuccessStatusCode)
                return null;

            return await response.Content.ReadFromJsonAsync<ConnectorPackageInfo>(JsonOptions, cancellationToken);
        }
        catch (HttpRequestException)
        {
            return null;
        }
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<string>> GetVersionsAsync(
        string packageId,
        CancellationToken cancellationToken = default)
    {
        var package = await GetPackageAsync(packageId, null, cancellationToken);
        return package?.AvailableVersions ?? [];
    }

    /// <inheritdoc />
    public async Task<string> DownloadAsync(
        string packageId,
        string version,
        string targetDirectory,
        CancellationToken cancellationToken = default)
    {
        Directory.CreateDirectory(targetDirectory);

        // Determine download URL
        string downloadUrl;
        if (_mode == RepositoryMode.StaticIndex)
        {
            // Static mode: packages/{packageId}/{version}/{packageId}.{version}.nupkg
            downloadUrl = $"{Source}/packages/{packageId}/{version}/{packageId}.{version}.nupkg";
        }
        else
        {
            // REST API mode: api/packages/{packageId}/{version}/download
            downloadUrl = $"{Source}/api/packages/{Uri.EscapeDataString(packageId)}/{Uri.EscapeDataString(version)}/download";
        }

        var targetPath = Path.Combine(targetDirectory, $"{packageId}.{version}.nupkg");

        using var response = await _httpClient.GetAsync(downloadUrl, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        response.EnsureSuccessStatusCode();

        await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
        await using var fileStream = File.Create(targetPath);
        await stream.CopyToAsync(fileStream, cancellationToken);

        return targetPath;
    }

    /// <summary>
    /// Downloads a package directly from a URL.
    /// </summary>
    /// <param name="url">Direct download URL.</param>
    /// <param name="targetDirectory">Target directory.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Path to the downloaded file.</returns>
    public async Task<string> DownloadFromUrlAsync(
        string url,
        string targetDirectory,
        CancellationToken cancellationToken = default)
    {
        Directory.CreateDirectory(targetDirectory);

        // Extract filename from URL or use a default
        var uri = new Uri(url);
        var fileName = Path.GetFileName(uri.LocalPath);
        if (string.IsNullOrEmpty(fileName) || !fileName.EndsWith(".nupkg", StringComparison.OrdinalIgnoreCase) &&
            !fileName.EndsWith(".swpkg", StringComparison.OrdinalIgnoreCase))
        {
            fileName = $"package-{Guid.NewGuid()}.swpkg";
        }

        var targetPath = Path.Combine(targetDirectory, fileName);

        using var response = await _httpClient.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        response.EnsureSuccessStatusCode();

        await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
        await using var fileStream = File.Create(targetPath);
        await stream.CopyToAsync(fileStream, cancellationToken);

        return targetPath;
    }

    private async Task<IReadOnlyList<ConnectorPackageInfo>> SearchStaticIndexAsync(
        string? query,
        int skip,
        int take,
        CancellationToken cancellationToken)
    {
        var packages = await GetAllPackagesAsync(cancellationToken);

        var filtered = packages.Where(p =>
            p.PackageId.StartsWith(_packagePrefix, StringComparison.OrdinalIgnoreCase));

        if (!string.IsNullOrWhiteSpace(query))
        {
            var lowerQuery = query.ToLowerInvariant();
            filtered = filtered.Where(p =>
                p.PackageId.Contains(lowerQuery, StringComparison.OrdinalIgnoreCase) ||
                p.Name.Contains(lowerQuery, StringComparison.OrdinalIgnoreCase) ||
                (p.Description?.Contains(lowerQuery, StringComparison.OrdinalIgnoreCase) ?? false) ||
                p.Tags.Any(t => t.Contains(lowerQuery, StringComparison.OrdinalIgnoreCase)));
        }

        return filtered
            .OrderByDescending(p => p.DownloadCount)
            .Skip(skip)
            .Take(take)
            .ToList();
    }

    private async Task<IReadOnlyList<ConnectorPackageInfo>> SearchRestApiAsync(
        string? query,
        int skip,
        int take,
        CancellationToken cancellationToken)
    {
        var url = $"{Source}/api/packages?skip={skip}&take={take}";
        if (!string.IsNullOrWhiteSpace(query))
        {
            url += $"&q={Uri.EscapeDataString(query)}";
        }

        try
        {
            var response = await _httpClient.GetAsync(url, cancellationToken);
            response.EnsureSuccessStatusCode();

            var packages = await response.Content.ReadFromJsonAsync<List<ConnectorPackageInfo>>(JsonOptions, cancellationToken);
            return packages ?? [];
        }
        catch (HttpRequestException)
        {
            return [];
        }
    }

    private async Task<List<ConnectorPackageInfo>> GetAllPackagesAsync(CancellationToken cancellationToken)
    {
        // Check cache
        if (_cachedPackages != null && DateTime.UtcNow < _cacheExpiry)
        {
            return _cachedPackages;
        }

        var indexUrl = $"{Source}/packages.json";

        try
        {
            var response = await _httpClient.GetAsync(indexUrl, cancellationToken);
            response.EnsureSuccessStatusCode();

            var packages = await response.Content.ReadFromJsonAsync<List<ConnectorPackageInfo>>(JsonOptions, cancellationToken);
            _cachedPackages = packages ?? [];
            _cacheExpiry = DateTime.UtcNow.Add(CacheDuration);

            return _cachedPackages;
        }
        catch (HttpRequestException)
        {
            return [];
        }
    }

    private static RepositoryMode DetectMode(string source)
    {
        // If source ends with specific API paths, use REST mode
        if (source.EndsWith("/api", StringComparison.OrdinalIgnoreCase) ||
            source.Contains("/api/", StringComparison.OrdinalIgnoreCase))
        {
            return RepositoryMode.RestApi;
        }

        // Default to static index mode
        return RepositoryMode.StaticIndex;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_ownsHttpClient)
        {
            _httpClient.Dispose();
        }
    }

    private enum RepositoryMode
    {
        StaticIndex,
        RestApi
    }
}
