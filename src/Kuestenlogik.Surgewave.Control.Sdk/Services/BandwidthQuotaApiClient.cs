using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using Kuestenlogik.Surgewave.Control.Models;

namespace Kuestenlogik.Surgewave.Control.Services;

/// <summary>
/// Client for Bandwidth Quota management REST API.
/// </summary>
public sealed class BandwidthQuotaApiClient : IBandwidthQuotaApiClient
{
    private readonly HttpClient _httpClient;
    private readonly JsonSerializerOptions _jsonOptions;

    public BandwidthQuotaApiClient(HttpClient httpClient)
    {
        _httpClient = httpClient;
        _jsonOptions = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true,
            Converters = { new JsonStringEnumConverter() }
        };
    }

    public async Task<IReadOnlyList<BandwidthUsageModel>> ListAllUsageAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await _httpClient.GetFromJsonAsync<BandwidthQuotaListModel>("/api/quotas/bandwidth", _jsonOptions, cancellationToken);
            return response?.Clients ?? [];
        }
        catch (Exception)
        {
            return [];
        }
    }

    public async Task<BandwidthUsageModel?> GetClientUsageAsync(string clientId, CancellationToken cancellationToken = default)
    {
        try
        {
            return await _httpClient.GetFromJsonAsync<BandwidthUsageModel>(
                $"/api/quotas/bandwidth/{Uri.EscapeDataString(clientId)}", _jsonOptions, cancellationToken);
        }
        catch (Exception)
        {
            return null;
        }
    }

    public async Task<bool> SetClientQuotaAsync(string clientId, long produceBytesPerSec, long consumeBytesPerSec, CancellationToken cancellationToken = default)
    {
        try
        {
            var request = new { ProduceBytesPerSec = produceBytesPerSec, ConsumeBytesPerSec = consumeBytesPerSec };
            var response = await _httpClient.PutAsJsonAsync(
                $"/api/quotas/bandwidth/client/{Uri.EscapeDataString(clientId)}", request, _jsonOptions, cancellationToken);
            return response.IsSuccessStatusCode;
        }
        catch (Exception)
        {
            return false;
        }
    }

    public async Task<bool> SetUserQuotaAsync(string user, long produceBytesPerSec, long consumeBytesPerSec, CancellationToken cancellationToken = default)
    {
        try
        {
            var request = new { ProduceBytesPerSec = produceBytesPerSec, ConsumeBytesPerSec = consumeBytesPerSec };
            var response = await _httpClient.PutAsJsonAsync(
                $"/api/quotas/bandwidth/user/{Uri.EscapeDataString(user)}", request, _jsonOptions, cancellationToken);
            return response.IsSuccessStatusCode;
        }
        catch (Exception)
        {
            return false;
        }
    }

    public async Task<bool> RemoveClientQuotaAsync(string clientId, CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await _httpClient.DeleteAsync(
                $"/api/quotas/bandwidth/client/{Uri.EscapeDataString(clientId)}", cancellationToken);
            return response.IsSuccessStatusCode;
        }
        catch (Exception)
        {
            return false;
        }
    }

    public async Task<BandwidthQuotaMetricsModel?> GetMetricsAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            return await _httpClient.GetFromJsonAsync<BandwidthQuotaMetricsModel>(
                "/api/quotas/bandwidth/metrics", _jsonOptions, cancellationToken);
        }
        catch (Exception)
        {
            return null;
        }
    }

    public async Task<BandwidthQuotaConfigModel?> GetConfigAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            return await _httpClient.GetFromJsonAsync<BandwidthQuotaConfigModel>(
                "/api/quotas/bandwidth/config", _jsonOptions, cancellationToken);
        }
        catch (Exception)
        {
            return null;
        }
    }
}
