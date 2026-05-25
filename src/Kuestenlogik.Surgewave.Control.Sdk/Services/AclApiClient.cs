using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using Kuestenlogik.Surgewave.Control.Models.Security;

namespace Kuestenlogik.Surgewave.Control.Services;

/// <summary>
/// Client for ACL management REST API.
/// </summary>
public sealed class AclApiClient : IAclApiClient
{
    private readonly HttpClient _httpClient;
    private readonly JsonSerializerOptions _jsonOptions;

    public AclApiClient(HttpClient httpClient)
    {
        _httpClient = httpClient;
        _jsonOptions = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true,
            Converters = { new JsonStringEnumConverter() }
        };
    }

    public async Task<IReadOnlyList<AclEntryModel>> ListAclsAsync(
        string? principal = null,
        AclResourceType? resourceType = null,
        string? resourceName = null,
        AclOperation? operation = null,
        AclPermission? permission = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var queryParams = new List<string>();

            if (principal != null)
                queryParams.Add($"principal={Uri.EscapeDataString(principal)}");
            if (resourceType.HasValue)
                queryParams.Add($"resourceType={resourceType.Value}");
            if (resourceName != null)
                queryParams.Add($"resourceName={Uri.EscapeDataString(resourceName)}");
            if (operation.HasValue)
                queryParams.Add($"operation={operation.Value}");
            if (permission.HasValue)
                queryParams.Add($"permission={permission.Value}");

            var url = queryParams.Count > 0
                ? $"/admin/acls/filter?{string.Join("&", queryParams)}"
                : "/admin/acls";

            var response = await _httpClient.GetFromJsonAsync<List<AclEntryModel>>(url, _jsonOptions, cancellationToken);
            return response ?? [];
        }
        catch (Exception)
        {
            return [];
        }
    }

    public async Task<AclEntryModel?> CreateAclAsync(CreateAclRequest request, CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await _httpClient.PostAsJsonAsync("/admin/acls", request, _jsonOptions, cancellationToken);
            if (!response.IsSuccessStatusCode)
                return null;

            return await response.Content.ReadFromJsonAsync<AclEntryModel>(_jsonOptions, cancellationToken);
        }
        catch (Exception)
        {
            return null;
        }
    }

    public async Task<IReadOnlyList<AclEntryModel>?> CreateAclsBatchAsync(IReadOnlyList<CreateAclRequest> requests, CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await _httpClient.PostAsJsonAsync("/admin/acls/batch", requests, _jsonOptions, cancellationToken);
            if (!response.IsSuccessStatusCode)
                return null;

            return await response.Content.ReadFromJsonAsync<List<AclEntryModel>>(_jsonOptions, cancellationToken);
        }
        catch (Exception)
        {
            return null;
        }
    }

    public async Task<int> DeleteAclsAsync(
        string? principal = null,
        AclResourceType? resourceType = null,
        string? resourceName = null,
        AclOperation? operation = null,
        AclPermission? permission = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var queryParams = new List<string>();

            if (principal != null)
                queryParams.Add($"principal={Uri.EscapeDataString(principal)}");
            if (resourceType.HasValue)
                queryParams.Add($"resourceType={resourceType.Value}");
            if (resourceName != null)
                queryParams.Add($"resourceName={Uri.EscapeDataString(resourceName)}");
            if (operation.HasValue)
                queryParams.Add($"operation={operation.Value}");
            if (permission.HasValue)
                queryParams.Add($"permission={permission.Value}");

            var url = queryParams.Count > 0
                ? $"/admin/acls?{string.Join("&", queryParams)}"
                : "/admin/acls";

            var response = await _httpClient.DeleteAsync(url, cancellationToken);
            if (!response.IsSuccessStatusCode)
                return 0;

            var result = await response.Content.ReadFromJsonAsync<AclDeleteResult>(_jsonOptions, cancellationToken);
            return result?.DeletedCount ?? 0;
        }
        catch (Exception)
        {
            return 0;
        }
    }
}
