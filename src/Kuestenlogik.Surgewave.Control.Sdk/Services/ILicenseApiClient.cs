using Kuestenlogik.Surgewave.Control.Models.License;

namespace Kuestenlogik.Surgewave.Control.Services;

/// <summary>
/// Read-only client for the broker's <c>/api/license/*</c> surface.
/// Powers the Control License page (G — Roadmap "Control UI license
/// page"): edition / licensee / expiry plus the per-plugin licence
/// requirement and activation status.
/// </summary>
public interface ILicenseApiClient
{
    /// <summary>Fetch edition, licensee, expiry. Returns <c>null</c> if
    /// the broker is unreachable or returned a non-200 response.</summary>
    Task<LicenseStatusModel?> GetStatusAsync(CancellationToken cancellationToken = default);

    /// <summary>Fetch the per-plugin licence + activation table. Returns
    /// an empty list if the broker is unreachable or has no plugins.</summary>
    Task<IReadOnlyList<LicensePluginRowModel>> GetPluginsAsync(CancellationToken cancellationToken = default);
}
