using System.Net.Http.Json;
using System.Text.Json;
using Kuestenlogik.Surgewave.Control.Models;

namespace Kuestenlogik.Surgewave.Control.Services;

/// <summary>
/// Client for the broker's Key-Value Store REST API (/api/kv/buckets).
/// </summary>
public sealed class KvApiClient : IKvApiClient
{
    private readonly HttpClient _httpClient;
    private readonly JsonSerializerOptions _jsonOptions;

    public KvApiClient(HttpClient httpClient)
    {
        _httpClient = httpClient;
        _jsonOptions = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
    }

    public async Task<IReadOnlyList<KvBucketModel>> ListBucketsAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var buckets = await _httpClient.GetFromJsonAsync<List<KvBucketModel>>(
                "/api/kv/buckets/", _jsonOptions, cancellationToken);
            return buckets ?? [];
        }
        catch (Exception)
        {
            return [];
        }
    }

    public async Task<KvBucketModel?> CreateBucketAsync(CreateKvBucketRequest request, CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await _httpClient.PostAsJsonAsync("/api/kv/buckets/", request, _jsonOptions, cancellationToken);
            if (!response.IsSuccessStatusCode)
                return null;

            return await response.Content.ReadFromJsonAsync<KvBucketModel>(_jsonOptions, cancellationToken);
        }
        catch (Exception)
        {
            return null;
        }
    }

    public async Task<bool> DeleteBucketAsync(string bucket, CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await _httpClient.DeleteAsync(
                $"/api/kv/buckets/{Uri.EscapeDataString(bucket)}", cancellationToken);
            return response.IsSuccessStatusCode;
        }
        catch (Exception)
        {
            return false;
        }
    }

    public async Task<IReadOnlyList<string>> ListKeysAsync(string bucket, CancellationToken cancellationToken = default)
    {
        try
        {
            var keys = await _httpClient.GetFromJsonAsync<List<string>>(
                $"/api/kv/buckets/{Uri.EscapeDataString(bucket)}/keys", _jsonOptions, cancellationToken);
            return keys ?? [];
        }
        catch (Exception)
        {
            return [];
        }
    }

    public async Task<KvEntryModel?> GetEntryAsync(string bucket, string key, CancellationToken cancellationToken = default)
    {
        try
        {
            return await _httpClient.GetFromJsonAsync<KvEntryModel>(
                $"/api/kv/buckets/{Uri.EscapeDataString(bucket)}/keys/{Uri.EscapeDataString(key)}",
                _jsonOptions, cancellationToken);
        }
        catch (Exception)
        {
            return null;
        }
    }

    public async Task<KvEntryModel?> PutEntryAsync(string bucket, string key, byte[] value, CancellationToken cancellationToken = default)
    {
        try
        {
            using var content = new ByteArrayContent(value);
            var response = await _httpClient.PutAsync(
                $"/api/kv/buckets/{Uri.EscapeDataString(bucket)}/keys/{Uri.EscapeDataString(key)}",
                content, cancellationToken);
            if (!response.IsSuccessStatusCode)
                return null;

            return await response.Content.ReadFromJsonAsync<KvEntryModel>(_jsonOptions, cancellationToken);
        }
        catch (Exception)
        {
            return null;
        }
    }

    public async Task<KvEntryModel?> DeleteEntryAsync(string bucket, string key, CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await _httpClient.DeleteAsync(
                $"/api/kv/buckets/{Uri.EscapeDataString(bucket)}/keys/{Uri.EscapeDataString(key)}",
                cancellationToken);
            if (!response.IsSuccessStatusCode)
                return null;

            return await response.Content.ReadFromJsonAsync<KvEntryModel>(_jsonOptions, cancellationToken);
        }
        catch (Exception)
        {
            return null;
        }
    }

    public async Task<IReadOnlyList<KvEntryModel>> GetHistoryAsync(string bucket, string key, CancellationToken cancellationToken = default)
    {
        try
        {
            var history = await _httpClient.GetFromJsonAsync<List<KvEntryModel>>(
                $"/api/kv/buckets/{Uri.EscapeDataString(bucket)}/keys/{Uri.EscapeDataString(key)}/history",
                _jsonOptions, cancellationToken);
            return history ?? [];
        }
        catch (Exception)
        {
            return [];
        }
    }
}
