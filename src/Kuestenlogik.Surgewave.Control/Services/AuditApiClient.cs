using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using Kuestenlogik.Surgewave.Control.Models;

namespace Kuestenlogik.Surgewave.Control.Services;

/// <summary>
/// Client for Audit REST API.
/// </summary>
public sealed class AuditApiClient : IAuditApiClient
{
    private readonly HttpClient _httpClient;
    private readonly JsonSerializerOptions _jsonOptions;

    public AuditApiClient(HttpClient httpClient)
    {
        _httpClient = httpClient;
        _jsonOptions = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true,
            Converters = { new JsonStringEnumConverter() }
        };
    }

    public async Task<AuditQueryResult> QueryEventsAsync(
        DateTimeOffset? start = null,
        DateTimeOffset? end = null,
        string? eventType = null,
        string? principal = null,
        string? resourceType = null,
        string? resourceName = null,
        bool? success = null,
        int limit = 100,
        int offset = 0,
        bool fromFile = false,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var queryParams = new List<string>();

            if (start.HasValue)
                queryParams.Add($"start={start.Value.ToUnixTimeMilliseconds()}");
            if (end.HasValue)
                queryParams.Add($"end={end.Value.ToUnixTimeMilliseconds()}");
            if (eventType != null)
                queryParams.Add($"type={Uri.EscapeDataString(eventType)}");
            if (principal != null)
                queryParams.Add($"principal={Uri.EscapeDataString(principal)}");
            if (resourceType != null)
                queryParams.Add($"resourceType={Uri.EscapeDataString(resourceType)}");
            if (resourceName != null)
                queryParams.Add($"resourceName={Uri.EscapeDataString(resourceName)}");
            if (success.HasValue)
                queryParams.Add($"success={success.Value.ToString().ToLowerInvariant()}");

            queryParams.Add($"limit={limit}");
            queryParams.Add($"offset={offset}");
            queryParams.Add($"fromFile={fromFile.ToString().ToLowerInvariant()}");

            var url = queryParams.Count > 0
                ? $"/admin/audit?{string.Join("&", queryParams)}"
                : "/admin/audit";

            var response = await _httpClient.GetFromJsonAsync<AuditQueryResult>(url, _jsonOptions, cancellationToken);
            return response ?? new AuditQueryResult();
        }
        catch (Exception)
        {
            return new AuditQueryResult();
        }
    }

    public async Task<AuditStatsModel?> GetStatsAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            return await _httpClient.GetFromJsonAsync<AuditStatsModel>("/admin/audit/stats", _jsonOptions, cancellationToken);
        }
        catch (Exception)
        {
            return null;
        }
    }

    public async Task<AuditConfigModel?> GetConfigAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            return await _httpClient.GetFromJsonAsync<AuditConfigModel>("/admin/audit/config", _jsonOptions, cancellationToken);
        }
        catch (Exception)
        {
            return null;
        }
    }
}
