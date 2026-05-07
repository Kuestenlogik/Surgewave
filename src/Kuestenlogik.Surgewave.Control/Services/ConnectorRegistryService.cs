using System.Net.Http.Json;

namespace Kuestenlogik.Surgewave.Control.Services;

public sealed class ConnectorRegistryService : IConnectorRegistryService
{
    private readonly HttpClient _httpClient;
    private List<ConnectorTypeInfo>? _cachedTypes;
    private DateTime _cacheExpiry;
    private static readonly TimeSpan CacheDuration = TimeSpan.FromSeconds(60);

    public ConnectorRegistryService(HttpClient httpClient)
    {
        _httpClient = httpClient;
    }

    public async Task<IReadOnlyList<ConnectorTypeInfo>> GetConnectorTypesAsync(CancellationToken cancellationToken = default)
    {
        if (_cachedTypes != null && DateTime.UtcNow < _cacheExpiry)
        {
            return _cachedTypes;
        }

        var response = await _httpClient.GetFromJsonAsync<List<ConnectorTypeInfo>>("/api/connectors", cancellationToken);
        _cachedTypes = response ?? [];
        _cacheExpiry = DateTime.UtcNow.Add(CacheDuration);
        return _cachedTypes;
    }

    /// <summary>
    /// Invalidates the cached connector types, forcing a fresh fetch on next call.
    /// </summary>
    public void InvalidateCache()
    {
        _cachedTypes = null;
    }

    public async Task<ConnectorConfigSchema?> GetConfigSchemaAsync(string connectorType, CancellationToken cancellationToken = default)
    {
        try
        {
            var encodedType = Uri.EscapeDataString(connectorType);
            return await _httpClient.GetFromJsonAsync<ConnectorConfigSchema>($"/api/connectors/{encodedType}/config", cancellationToken);
        }
        catch (HttpRequestException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return null;
        }
    }
}
