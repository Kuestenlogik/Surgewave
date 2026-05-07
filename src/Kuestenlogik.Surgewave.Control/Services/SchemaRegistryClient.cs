using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using Kuestenlogik.Surgewave.Control.Models;

namespace Kuestenlogik.Surgewave.Control.Services;

/// <summary>
/// Client for Schema Registry REST API.
/// </summary>
public sealed class SchemaRegistryClient : ISchemaRegistryClient
{
    private readonly HttpClient _httpClient;
    private readonly JsonSerializerOptions _jsonOptions;

    public SchemaRegistryClient(HttpClient httpClient)
    {
        _httpClient = httpClient;
        _jsonOptions = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true,
            Converters = { new JsonStringEnumConverter() }
        };
    }

    public async Task<IReadOnlyList<string>> GetSubjectsAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await _httpClient.GetFromJsonAsync<List<string>>("/subjects", _jsonOptions, cancellationToken);
            return response ?? [];
        }
        catch (Exception)
        {
            return [];
        }
    }

    public async Task<IReadOnlyList<int>> GetVersionsAsync(string subject, CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await _httpClient.GetFromJsonAsync<List<int>>($"/subjects/{Uri.EscapeDataString(subject)}/versions", _jsonOptions, cancellationToken);
            return response ?? [];
        }
        catch (Exception)
        {
            return [];
        }
    }

    public async Task<SchemaModel?> GetSchemaAsync(string subject, int version, CancellationToken cancellationToken = default)
    {
        try
        {
            return await _httpClient.GetFromJsonAsync<SchemaModel>($"/subjects/{Uri.EscapeDataString(subject)}/versions/{version}", _jsonOptions, cancellationToken);
        }
        catch (Exception)
        {
            return null;
        }
    }

    public async Task<SchemaModel?> GetLatestSchemaAsync(string subject, CancellationToken cancellationToken = default)
    {
        try
        {
            return await _httpClient.GetFromJsonAsync<SchemaModel>($"/subjects/{Uri.EscapeDataString(subject)}/versions/latest", _jsonOptions, cancellationToken);
        }
        catch (Exception)
        {
            return null;
        }
    }

    public async Task<int?> RegisterSchemaAsync(string subject, RegisterSchemaRequest request, CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await _httpClient.PostAsJsonAsync($"/subjects/{Uri.EscapeDataString(subject)}/versions", request, _jsonOptions, cancellationToken);
            if (!response.IsSuccessStatusCode)
                return null;

            var result = await response.Content.ReadFromJsonAsync<RegisterSchemaResponse>(_jsonOptions, cancellationToken);
            return result?.Id;
        }
        catch (Exception)
        {
            return null;
        }
    }

    public async Task<IReadOnlyList<int>?> DeleteSubjectAsync(string subject, bool permanent = false, CancellationToken cancellationToken = default)
    {
        try
        {
            var url = permanent
                ? $"/subjects/{Uri.EscapeDataString(subject)}?permanent=true"
                : $"/subjects/{Uri.EscapeDataString(subject)}";

            var response = await _httpClient.DeleteAsync(url, cancellationToken);
            if (!response.IsSuccessStatusCode)
                return null;

            return await response.Content.ReadFromJsonAsync<List<int>>(_jsonOptions, cancellationToken);
        }
        catch (Exception)
        {
            return null;
        }
    }

    public async Task<int?> DeleteVersionAsync(string subject, int version, bool permanent = false, CancellationToken cancellationToken = default)
    {
        try
        {
            var url = permanent
                ? $"/subjects/{Uri.EscapeDataString(subject)}/versions/{version}?permanent=true"
                : $"/subjects/{Uri.EscapeDataString(subject)}/versions/{version}";

            var response = await _httpClient.DeleteAsync(url, cancellationToken);
            if (!response.IsSuccessStatusCode)
                return null;

            return await response.Content.ReadFromJsonAsync<int>(_jsonOptions, cancellationToken);
        }
        catch (Exception)
        {
            return null;
        }
    }

    public async Task<CompatibilityCheckResult?> CheckCompatibilityAsync(string subject, RegisterSchemaRequest request, CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await _httpClient.PostAsJsonAsync($"/compatibility/subjects/{Uri.EscapeDataString(subject)}/versions/latest", request, _jsonOptions, cancellationToken);
            if (!response.IsSuccessStatusCode)
                return null;

            return await response.Content.ReadFromJsonAsync<CompatibilityCheckResult>(_jsonOptions, cancellationToken);
        }
        catch (Exception)
        {
            return null;
        }
    }

    public async Task<string?> GetGlobalCompatibilityAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await _httpClient.GetFromJsonAsync<CompatibilityConfigModel>("/config", _jsonOptions, cancellationToken);
            return response?.CompatibilityLevel;
        }
        catch (Exception)
        {
            return null;
        }
    }

    public async Task<bool> SetGlobalCompatibilityAsync(string compatibility, CancellationToken cancellationToken = default)
    {
        try
        {
            var request = new { compatibility };
            var response = await _httpClient.PutAsJsonAsync("/config", request, _jsonOptions, cancellationToken);
            return response.IsSuccessStatusCode;
        }
        catch (Exception)
        {
            return false;
        }
    }

    public async Task<string?> GetSubjectCompatibilityAsync(string subject, CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await _httpClient.GetFromJsonAsync<CompatibilityConfigModel>($"/config/{Uri.EscapeDataString(subject)}", _jsonOptions, cancellationToken);
            return response?.CompatibilityLevel;
        }
        catch (Exception)
        {
            return null;
        }
    }

    public async Task<bool> SetSubjectCompatibilityAsync(string subject, string compatibility, CancellationToken cancellationToken = default)
    {
        try
        {
            var request = new { compatibility };
            var response = await _httpClient.PutAsJsonAsync($"/config/{Uri.EscapeDataString(subject)}", request, _jsonOptions, cancellationToken);
            return response.IsSuccessStatusCode;
        }
        catch (Exception)
        {
            return false;
        }
    }

    public async Task<IReadOnlyList<string>> GetSchemaTypesAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await _httpClient.GetFromJsonAsync<List<string>>("/schemas/types", _jsonOptions, cancellationToken);
            return response ?? [];
        }
        catch (Exception)
        {
            return [];
        }
    }

    public async Task<SchemaByIdModel?> GetSchemaByIdAsync(int id, CancellationToken cancellationToken = default)
    {
        try
        {
            return await _httpClient.GetFromJsonAsync<SchemaByIdModel>($"/schemas/ids/{id}", _jsonOptions, cancellationToken);
        }
        catch (Exception)
        {
            return null;
        }
    }

    public async Task<IReadOnlyList<SubjectVersionModel>> GetSchemaVersionsByIdAsync(int id, CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await _httpClient.GetFromJsonAsync<List<SubjectVersionModel>>($"/schemas/ids/{id}/versions", _jsonOptions, cancellationToken);
            return response ?? [];
        }
        catch (Exception)
        {
            return [];
        }
    }

    public async Task<InferredSchemaModel?> InferSchemaAsync(string topic, int? sampleSize = null, CancellationToken cancellationToken = default)
    {
        try
        {
            var url = $"/schemas/infer/{Uri.EscapeDataString(topic)}";
            if (sampleSize.HasValue)
            {
                url += $"?sample={sampleSize.Value}";
            }
            return await _httpClient.GetFromJsonAsync<InferredSchemaModel>(url, _jsonOptions, cancellationToken);
        }
        catch (Exception)
        {
            return null;
        }
    }

    public async Task<int?> InferAndRegisterSchemaAsync(string topic, CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await _httpClient.PostAsync($"/schemas/infer/{Uri.EscapeDataString(topic)}/register", null, cancellationToken);
            if (!response.IsSuccessStatusCode)
                return null;

            var result = await response.Content.ReadFromJsonAsync<RegisterSchemaResponse>(_jsonOptions, cancellationToken);
            return result?.Id;
        }
        catch (Exception)
        {
            return null;
        }
    }

    public async Task<IReadOnlyList<InferredSchemaSummaryModel>> GetInferredSchemasAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await _httpClient.GetFromJsonAsync<List<InferredSchemaSummaryModel>>("/schemas/inferred", _jsonOptions, cancellationToken);
            return response ?? [];
        }
        catch (Exception)
        {
            return [];
        }
    }
}
