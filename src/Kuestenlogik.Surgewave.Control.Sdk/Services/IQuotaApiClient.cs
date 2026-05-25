using Kuestenlogik.Surgewave.Control.Models;

namespace Kuestenlogik.Surgewave.Control.Services;

/// <summary>
/// Client for Quota management REST API.
/// </summary>
public interface IQuotaApiClient
{
    /// <summary>
    /// Get current quota configuration.
    /// </summary>
    Task<QuotaConfigModel?> GetConfigAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Update quota configuration.
    /// </summary>
    Task<QuotaConfigModel?> UpdateConfigAsync(UpdateQuotaConfigRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// List all clients with their quota statistics.
    /// </summary>
    Task<IReadOnlyList<ClientQuotaStatsModel>> ListClientStatsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Get quota statistics for a specific client.
    /// </summary>
    Task<ClientQuotaStatsModel?> GetClientStatsAsync(string clientId, CancellationToken cancellationToken = default);
}
