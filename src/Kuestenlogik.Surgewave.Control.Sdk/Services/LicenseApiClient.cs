using System.Net.Http.Json;
using System.Text.Json;
using Kuestenlogik.Surgewave.Control.Models.License;

namespace Kuestenlogik.Surgewave.Control.Services;

/// <summary>HTTP-backed <see cref="ILicenseApiClient"/>.</summary>
public sealed class LicenseApiClient : ILicenseApiClient
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    private readonly HttpClient _http;

    public LicenseApiClient(HttpClient http) => _http = http;

    public async Task<LicenseStatusModel?> GetStatusAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            return await _http.GetFromJsonAsync<LicenseStatusModel>("/api/license/status", JsonOptions, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception)
        {
            return null;
        }
    }

    public async Task<IReadOnlyList<LicensePluginRowModel>> GetPluginsAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var rows = await _http.GetFromJsonAsync<List<LicensePluginRowModel>>("/api/license/plugins", JsonOptions, cancellationToken).ConfigureAwait(false);
            return (IReadOnlyList<LicensePluginRowModel>?)rows ?? [];
        }
        catch (Exception)
        {
            return [];
        }
    }
}
