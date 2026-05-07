using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using Kuestenlogik.Surgewave.Control.Models;

namespace Kuestenlogik.Surgewave.Control.Services;

/// <summary>
/// Client for Quota management REST API.
/// </summary>
public sealed class QuotaApiClient : IQuotaApiClient
{
    private readonly HttpClient _httpClient;
    private readonly JsonSerializerOptions _jsonOptions;

    public QuotaApiClient(HttpClient httpClient)
    {
        _httpClient = httpClient;
        _jsonOptions = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true,
            Converters = { new JsonStringEnumConverter() }
        };
    }

    public async Task<QuotaConfigModel?> GetConfigAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            return await _httpClient.GetFromJsonAsync<QuotaConfigModel>("/admin/quotas/config", _jsonOptions, cancellationToken);
        }
        catch (Exception)
        {
            return null;
        }
    }

    public async Task<QuotaConfigModel?> UpdateConfigAsync(UpdateQuotaConfigRequest request, CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await _httpClient.PutAsJsonAsync("/admin/quotas/config", request, _jsonOptions, cancellationToken);
            if (!response.IsSuccessStatusCode)
                return null;

            return await response.Content.ReadFromJsonAsync<QuotaConfigModel>(_jsonOptions, cancellationToken);
        }
        catch (Exception)
        {
            return null;
        }
    }

    public async Task<IReadOnlyList<ClientQuotaStatsModel>> ListClientStatsAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await _httpClient.GetFromJsonAsync<List<ClientQuotaStatsModel>>("/admin/quotas/clients", _jsonOptions, cancellationToken);
            return response ?? [];
        }
        catch (Exception)
        {
            return [];
        }
    }

    public async Task<ClientQuotaStatsModel?> GetClientStatsAsync(string clientId, CancellationToken cancellationToken = default)
    {
        try
        {
            return await _httpClient.GetFromJsonAsync<ClientQuotaStatsModel>($"/admin/quotas/clients/{Uri.EscapeDataString(clientId)}", _jsonOptions, cancellationToken);
        }
        catch (Exception)
        {
            return null;
        }
    }
}
