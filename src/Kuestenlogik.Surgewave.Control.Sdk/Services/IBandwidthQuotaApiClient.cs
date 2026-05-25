using Kuestenlogik.Surgewave.Control.Models;

namespace Kuestenlogik.Surgewave.Control.Services;

/// <summary>
/// Client for Bandwidth Quota management REST API.
/// </summary>
public interface IBandwidthQuotaApiClient
{
    /// <summary>
    /// List all bandwidth quotas and current usage.
    /// </summary>
    Task<IReadOnlyList<BandwidthUsageModel>> ListAllUsageAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Get bandwidth usage for a specific client.
    /// </summary>
    Task<BandwidthUsageModel?> GetClientUsageAsync(string clientId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Set or update bandwidth quota for a client.
    /// </summary>
    Task<bool> SetClientQuotaAsync(string clientId, long produceBytesPerSec, long consumeBytesPerSec, CancellationToken cancellationToken = default);

    /// <summary>
    /// Set or update bandwidth quota for a user.
    /// </summary>
    Task<bool> SetUserQuotaAsync(string user, long produceBytesPerSec, long consumeBytesPerSec, CancellationToken cancellationToken = default);

    /// <summary>
    /// Remove client-specific bandwidth quota override.
    /// </summary>
    Task<bool> RemoveClientQuotaAsync(string clientId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Get aggregate bandwidth throttling metrics.
    /// </summary>
    Task<BandwidthQuotaMetricsModel?> GetMetricsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Get current bandwidth quota configuration.
    /// </summary>
    Task<BandwidthQuotaConfigModel?> GetConfigAsync(CancellationToken cancellationToken = default);
}
