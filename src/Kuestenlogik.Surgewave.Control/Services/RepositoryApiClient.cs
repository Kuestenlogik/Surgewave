using System.Net.Http.Json;
using Kuestenlogik.Surgewave.Plugins.Repository;

namespace Kuestenlogik.Surgewave.Control.Services;

public sealed class RepositoryApiClient : IRepositoryApiClient
{
    private const string BasePath = "api/plugins/repositories";

    private readonly HttpClient _http;
    private readonly ILogger<RepositoryApiClient> _logger;

    public RepositoryApiClient(HttpClient http, ILogger<RepositoryApiClient> logger)
    {
        _http = http;
        _logger = logger;
    }

    public async Task<IReadOnlyList<RepositoryEntry>> ListAsync(CancellationToken ct = default)
    {
        using var resp = await _http.GetAsync($"{BasePath}/", ct).ConfigureAwait(false);
        resp.EnsureSuccessStatusCode();
        var payload = await resp.Content.ReadFromJsonAsync<ListResponse>(ct).ConfigureAwait(false);
        return payload?.Repositories ?? [];
    }

    public async Task<RepositoryEntry> AddAsync(RepositoryEntry entry, CancellationToken ct = default)
    {
        using var resp = await _http.PostAsJsonAsync($"{BasePath}/", entry, ct).ConfigureAwait(false);
        if (!resp.IsSuccessStatusCode)
        {
            var body = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
            _logger.LogWarning("Add repository failed: {Status} {Body}", resp.StatusCode, body);
            throw new InvalidOperationException($"Add failed ({(int)resp.StatusCode}): {body}");
        }
        var saved = await resp.Content.ReadFromJsonAsync<RepositoryEntry>(ct).ConfigureAwait(false);
        return saved ?? throw new InvalidOperationException("Empty add response");
    }

    public async Task<RepositoryEntry> UpdateAsync(string name, RepositoryEntry entry, CancellationToken ct = default)
    {
        using var resp = await _http.PutAsJsonAsync($"{BasePath}/{Uri.EscapeDataString(name)}", entry, ct).ConfigureAwait(false);
        if (!resp.IsSuccessStatusCode)
        {
            var body = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
            throw new InvalidOperationException($"Update failed ({(int)resp.StatusCode}): {body}");
        }
        var saved = await resp.Content.ReadFromJsonAsync<RepositoryEntry>(ct).ConfigureAwait(false);
        return saved ?? throw new InvalidOperationException("Empty update response");
    }

    public async Task DeleteAsync(string name, CancellationToken ct = default)
    {
        using var resp = await _http.DeleteAsync($"{BasePath}/{Uri.EscapeDataString(name)}", ct).ConfigureAwait(false);
        if (!resp.IsSuccessStatusCode)
        {
            var body = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
            throw new InvalidOperationException($"Delete failed ({(int)resp.StatusCode}): {body}");
        }
    }

    private sealed record ListResponse(string ConfigPath, IReadOnlyList<RepositoryEntry> Repositories);
}
