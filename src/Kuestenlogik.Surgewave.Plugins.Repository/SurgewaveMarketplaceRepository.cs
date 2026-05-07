using System.Diagnostics.CodeAnalysis;
using System.Net.Http.Json;
using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Plugins.Repository;

/// <summary>
/// Connector repository backed by a Surgewave Marketplace Server.
/// Implements <see cref="IConnectorRepository"/> using the Marketplace v1 REST API.
/// </summary>
[SuppressMessage("Design", "CA1056:URI properties should not be strings", Justification = "Source URL stored as string")]
public sealed class SurgewaveMarketplaceRepository : IConnectorRepository, IDisposable
{
    private readonly HttpClient _httpClient;

    public string Name { get; }
    public string Source { get; }

    public SurgewaveMarketplaceRepository(string name, string sourceUrl)
    {
        Name = name;
        Source = sourceUrl.TrimEnd('/');
        _httpClient = new HttpClient { BaseAddress = new Uri(Source) };
    }

    public async Task<IReadOnlyList<ConnectorPackageInfo>> SearchAsync(
        string? query, int skip = 0, int take = 20, CancellationToken cancellationToken = default)
    {
        var url = $"/api/v1/search?skip={skip}&take={take}";
        if (!string.IsNullOrWhiteSpace(query))
            url += $"&q={Uri.EscapeDataString(query)}";

        var response = await _httpClient.GetFromJsonAsync<MarketplaceSearchResult>(url, cancellationToken);
        if (response?.Data == null) return [];

        return response.Data.Select(MapToPackageInfo).ToList();
    }

    public async Task<ConnectorPackageInfo?> GetPackageAsync(
        string packageId, string? version = null, CancellationToken cancellationToken = default)
    {
        // Get versions first if no specific version requested
        if (version == null)
        {
            var versions = await GetVersionsAsync(packageId, cancellationToken);
            version = versions.Count > 0 ? versions[^1] : null;
            if (version == null) return null;
        }

        try
        {
            var meta = await _httpClient.GetFromJsonAsync<MarketplacePackage>(
                $"/api/v1/packages/{packageId}/{version}/metadata", cancellationToken);

            return meta == null ? null : MapToPackageInfo(meta);
        }
        catch (HttpRequestException)
        {
            return null;
        }
    }

    public async Task<IReadOnlyList<string>> GetVersionsAsync(
        string packageId, CancellationToken cancellationToken = default)
    {
        try
        {
            var result = await _httpClient.GetFromJsonAsync<MarketplaceVersions>(
                $"/api/v1/packages/{packageId}/index.json", cancellationToken);

            return result?.Versions ?? [];
        }
        catch (HttpRequestException)
        {
            return [];
        }
    }

    public async Task<string> DownloadAsync(
        string packageId, string version, string targetDirectory,
        CancellationToken cancellationToken = default)
    {
        var response = await _httpClient.GetAsync(
            $"/api/v1/packages/{packageId}/{version}/download", cancellationToken);

        response.EnsureSuccessStatusCode();

        Directory.CreateDirectory(targetDirectory);
        var filePath = Path.Combine(targetDirectory, $"{packageId}-{version}.swpkg");

        await using var fileStream = new FileStream(filePath, FileMode.Create);
        await response.Content.CopyToAsync(fileStream, cancellationToken);

        return filePath;
    }

    public void Dispose() => _httpClient.Dispose();

    private static ConnectorPackageInfo MapToPackageInfo(MarketplacePackage pkg) => new()
    {
        PackageId = pkg.Id,
        Version = pkg.Version,
        Name = pkg.Name,
        Description = pkg.Description,
        Author = pkg.Authors?.Length > 0 ? string.Join(", ", pkg.Authors) : null,
        License = pkg.License,
        Tags = pkg.Tags ?? [],
        AvailableVersions = pkg.AllVersions ?? [],
        DownloadCount = pkg.DownloadCount,
        Published = pkg.PublishedAt,
        IsSigned = pkg.IsSigned,
        SignerIdentity = pkg.SignerIdentity,
        SignerProvider = pkg.SignerProvider
    };

    // DTOs matching Marketplace API responses
    private sealed class MarketplaceSearchResult
    {
        [JsonPropertyName("totalHits")]
        public int TotalHits { get; init; }

        [JsonPropertyName("data")]
        public MarketplacePackage[]? Data { get; init; }
    }

    private sealed class MarketplacePackage
    {
        [JsonPropertyName("id")]
        public string Id { get; init; } = "";

        [JsonPropertyName("version")]
        public string Version { get; init; } = "";

        [JsonPropertyName("name")]
        public string Name { get; init; } = "";

        [JsonPropertyName("description")]
        public string? Description { get; init; }

        [JsonPropertyName("authors")]
        public string[]? Authors { get; init; }

        [JsonPropertyName("tags")]
        public string[]? Tags { get; init; }

        [JsonPropertyName("license")]
        public string? License { get; init; }

        [JsonPropertyName("downloadCount")]
        public long DownloadCount { get; init; }

        [JsonPropertyName("publishedAt")]
        public DateTimeOffset PublishedAt { get; init; }

        [JsonPropertyName("allVersions")]
        public List<string>? AllVersions { get; init; }

        [JsonPropertyName("isSigned")]
        public bool IsSigned { get; init; }

        [JsonPropertyName("signerIdentity")]
        public string? SignerIdentity { get; init; }

        [JsonPropertyName("signerProvider")]
        public string? SignerProvider { get; init; }
    }

    private sealed class MarketplaceVersions
    {
        [JsonPropertyName("versions")]
        public string[]? Versions { get; init; }
    }
}
