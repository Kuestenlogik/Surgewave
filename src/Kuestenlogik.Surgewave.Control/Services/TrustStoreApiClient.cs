using System.Net.Http.Headers;
using System.Net.Http.Json;
using Kuestenlogik.Surgewave.Control.Models.TrustStore;

namespace Kuestenlogik.Surgewave.Control.Services;

/// <summary>
/// HttpClient-backed implementation that talks to the broker at the URL
/// configured via <c>Broker:ApiUrl</c> (default https://localhost:9093).
/// The HttpClient's <c>BaseAddress</c> is set by <c>ConfigureHttpClient</c>
/// in Program.cs alongside the other broker-API clients; this class uses
/// relative paths so a single config knob retargets every client. The
/// upload path goes through <c>multipart/form-data</c> so it matches the
/// broker's <c>ReadFormAsync</c> expectation.
/// </summary>
public sealed class TrustStoreApiClient : ITrustStoreApiClient
{
    private const string BasePath = "api/plugins/trusted-keys";

    private readonly HttpClient _http;
    private readonly ILogger<TrustStoreApiClient> _logger;

    public TrustStoreApiClient(HttpClient http, ILogger<TrustStoreApiClient> logger)
    {
        _http = http;
        _logger = logger;
    }

    public async Task<TrustStoreStatus> GetStatusAsync(CancellationToken ct = default)
    {
        using var resp = await _http.GetAsync($"{BasePath}/", ct).ConfigureAwait(false);
        if (resp.StatusCode == System.Net.HttpStatusCode.ServiceUnavailable)
        {
            // Broker has no trusted-keys-dir configured — return an empty
            // shape so the UI can render the "configure me" panel.
            return new TrustStoreStatus(TrustedKeysDir: null, RequireSigned: false, ProviderName: "builtin-ecdsa", Keys: []);
        }
        resp.EnsureSuccessStatusCode();
        var payload = await resp.Content.ReadFromJsonAsync<TrustStoreStatus>(ct).ConfigureAwait(false);
        return payload ?? new TrustStoreStatus(null, false, "builtin-ecdsa", []);
    }

    public async Task<TrustedKeyInfo> UploadAsync(string keyName, Stream pemContent, CancellationToken ct = default)
    {
        using var content = new MultipartFormDataContent();
        using var streamContent = new StreamContent(pemContent);
        streamContent.Headers.ContentType = new MediaTypeHeaderValue("application/x-pem-file");
        content.Add(streamContent, "file", $"{keyName}.pub");
        using var nameContent = new StringContent(keyName);
        content.Add(nameContent, "name");

        using var resp = await _http.PostAsync($"{BasePath}/upload", content, ct).ConfigureAwait(false);
        if (!resp.IsSuccessStatusCode)
        {
            var body = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
            _logger.LogWarning("Trusted-key upload failed: {Status} {Body}", resp.StatusCode, body);
            throw new InvalidOperationException($"Upload failed ({(int)resp.StatusCode}): {body}");
        }
        var info = await resp.Content.ReadFromJsonAsync<TrustedKeyInfo>(ct).ConfigureAwait(false);
        return info ?? throw new InvalidOperationException("Empty upload response");
    }

    public async Task DeleteAsync(string keyName, CancellationToken ct = default)
    {
        using var resp = await _http.DeleteAsync($"{BasePath}/{Uri.EscapeDataString(keyName)}", ct).ConfigureAwait(false);
        if (!resp.IsSuccessStatusCode)
        {
            var body = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
            throw new InvalidOperationException($"Delete failed ({(int)resp.StatusCode}): {body}");
        }
    }

    public async Task<GeneratedKeyPair> GenerateAsync(string keyName, CancellationToken ct = default)
    {
        using var resp = await _http.PostAsJsonAsync($"{BasePath}/generate", new { name = keyName }, ct).ConfigureAwait(false);
        if (!resp.IsSuccessStatusCode)
        {
            var body = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
            throw new InvalidOperationException($"Generate failed ({(int)resp.StatusCode}): {body}");
        }
        var pair = await resp.Content.ReadFromJsonAsync<GeneratedKeyPair>(ct).ConfigureAwait(false);
        return pair ?? throw new InvalidOperationException("Empty generate response");
    }
}
