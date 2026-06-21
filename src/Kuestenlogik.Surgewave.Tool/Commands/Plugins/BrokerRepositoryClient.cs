using System.Net;
using System.Net.Http.Json;
using Kuestenlogik.Surgewave.Plugins.Repository;

namespace Kuestenlogik.Surgewave.Cli.Commands.Plugins;

/// <summary>
/// Thin HTTP client for the broker's <c>/api/plugins/repositories</c>
/// surface. Lets <c>surgewave plugins repo</c> push edits straight to the
/// broker's canonical <c>surgewave-repositories.json</c> store instead of
/// writing to a CLI-local <c>~/.surgewave/surgewave-repositories.json</c>
/// that no other process reads.
///
/// Mirrors Control's <c>RepositoryApiClient</c> shape — same endpoints,
/// same <see cref="RepositoryEntry"/> JSON contract. Connection failures
/// surface as <see cref="BrokerUnreachableException"/> so callers can render
/// a clear "is the broker running on --broker-url?" message instead of a
/// generic socket trace.
/// </summary>
public sealed class BrokerRepositoryClient : IDisposable
{
    private const string BasePath = "api/plugins/repositories";

    private readonly HttpClient _http;
    private readonly bool _ownsHttp;

    public BrokerRepositoryClient(string baseUrl, TimeSpan? timeout = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(baseUrl);
        // Accept the broker's dev cert (https://localhost:9093 is dotnet
        // dev-certs by default). Production deployments should front the
        // broker with a trusted cert anyway; rejecting self-signed here
        // would just make the CLI unusable in dev.
        // HttpClient owns and disposes the handler via disposeHandler:true.
        // CA2000 can't see the ownership transfer through the ctor so suppress.
        HttpClientHandler? handler = null;
        try
        {
            handler = new HttpClientHandler
            {
                ServerCertificateCustomValidationCallback = (_, _, _, _) => true,
            };
            _http = new HttpClient(handler, disposeHandler: true)
            {
                BaseAddress = new Uri(baseUrl.TrimEnd('/') + "/"),
                Timeout = timeout ?? TimeSpan.FromSeconds(10),
            };
            handler = null; // ownership transferred
        }
        finally
        {
            handler?.Dispose();
        }
        _ownsHttp = true;
    }

    // Test seam: caller supplies a pre-configured HttpClient (typically
    // with a stub handler) so we can unit-test request shape without a
    // running broker.
    public BrokerRepositoryClient(HttpClient http)
    {
        _http = http ?? throw new ArgumentNullException(nameof(http));
        _ownsHttp = false;
    }

    public async Task<IReadOnlyList<RepositoryEntry>> ListAsync(CancellationToken ct = default)
    {
        var resp = await SendAsync(HttpMethod.Get, $"{BasePath}/", null, ct);
        var payload = await resp.Content.ReadFromJsonAsync<ListResponse>(ct);
        return payload?.Repositories ?? [];
    }

    public async Task<RepositoryEntry> AddAsync(RepositoryEntry entry, CancellationToken ct = default)
    {
        using var body = JsonContent.Create(entry);
        var resp = await SendAsync(HttpMethod.Post, $"{BasePath}/", body, ct);
        return await resp.Content.ReadFromJsonAsync<RepositoryEntry>(ct)
            ?? throw new InvalidOperationException("Broker returned an empty add response.");
    }

    public async Task<RepositoryEntry> UpdateAsync(string name, RepositoryEntry entry, CancellationToken ct = default)
    {
        using var body = JsonContent.Create(entry);
        var resp = await SendAsync(HttpMethod.Put, $"{BasePath}/{Uri.EscapeDataString(name)}", body, ct);
        return await resp.Content.ReadFromJsonAsync<RepositoryEntry>(ct)
            ?? throw new InvalidOperationException("Broker returned an empty update response.");
    }

    public async Task RemoveAsync(string name, CancellationToken ct = default)
    {
        await SendAsync(HttpMethod.Delete, $"{BasePath}/{Uri.EscapeDataString(name)}", null, ct);
    }

    private async Task<HttpResponseMessage> SendAsync(HttpMethod method, string path, HttpContent? body, CancellationToken ct)
    {
        // body is owned by the caller; do not Dispose here. HttpRequestMessage's
        // Dispose otherwise tears down the caller's still-live content.
        using var request = new HttpRequestMessage(method, path) { Content = body };
        HttpResponseMessage resp;
        try
        {
            resp = await _http.SendAsync(request, ct).ConfigureAwait(false);
        }
        catch (HttpRequestException ex)
        {
            throw new BrokerUnreachableException(_http.BaseAddress?.ToString() ?? "(unset)", ex);
        }
        catch (TaskCanceledException ex) when (!ct.IsCancellationRequested)
        {
            // TaskCanceledException with no caller-token cancellation = HttpClient timeout.
            throw new BrokerUnreachableException(_http.BaseAddress?.ToString() ?? "(unset)", ex);
        }

        if (resp.StatusCode == HttpStatusCode.NotFound)
        {
            resp.Dispose();
            throw new RepositoryNotFoundException(path);
        }
        if (!resp.IsSuccessStatusCode)
        {
            var bodyText = await resp.Content.ReadAsStringAsync(ct);
            resp.Dispose();
            throw new InvalidOperationException($"Broker rejected {method.Method} {path} ({(int)resp.StatusCode}): {bodyText}");
        }
        return resp;
    }

    public void Dispose()
    {
        if (_ownsHttp) _http.Dispose();
    }

    private sealed record ListResponse(string ConfigPath, IReadOnlyList<RepositoryEntry> Repositories);
}

public sealed class BrokerUnreachableException : Exception
{
    public BrokerUnreachableException(string url, Exception inner)
        : base($"Could not reach the broker at {url}. Is it running? Override with --broker-url or $SURGEWAVE_BROKER_URL.", inner) { }
}

public sealed class RepositoryNotFoundException : Exception
{
    public RepositoryNotFoundException(string path) : base($"Repository not found at {path}.") { }
}
