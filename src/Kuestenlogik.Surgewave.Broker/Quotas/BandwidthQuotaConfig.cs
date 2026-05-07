using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Core.Configuration;

namespace Kuestenlogik.Surgewave.Broker.Quotas;

/// <summary>
/// Configuration for per-client/user bandwidth throttling.
/// Limits how many bytes per second a client can produce or consume,
/// preventing a single client from saturating the network.
/// </summary>
public sealed class BandwidthQuotaConfig : IValidatableConfig
{
    /// <summary>
    /// Enable bandwidth quota enforcement.
    /// </summary>
    public bool Enabled { get; set; } = false;

    /// <summary>
    /// Default maximum produce bytes per second for any client.
    /// 0 = unlimited (no limit enforced).
    /// </summary>
    [Range(0, long.MaxValue)]
    public long DefaultProduceBytesPerSec { get; set; } = 0;

    /// <summary>
    /// Default maximum consume (fetch) bytes per second for any client.
    /// 0 = unlimited (no limit enforced).
    /// </summary>
    [Range(0, long.MaxValue)]
    public long DefaultConsumeBytesPerSec { get; set; } = 0;

    /// <summary>
    /// Per-client bandwidth quota overrides. Key is the client ID.
    /// These take precedence over default quotas.
    /// </summary>
    public Dictionary<string, ClientBandwidthQuota> ClientOverrides { get; set; } = [];

    /// <summary>
    /// Per-user bandwidth quota overrides. Key is the authenticated user name.
    /// User overrides take precedence over client overrides and defaults.
    /// </summary>
    public Dictionary<string, ClientBandwidthQuota> UserOverrides { get; set; } = [];

    /// <summary>
    /// Sliding window duration in milliseconds for rate measurement.
    /// Default: 1000ms (1 second).
    /// </summary>
    [Range(1, int.MaxValue)]
    public int EnforcementWindowMs { get; set; } = 1000;

    /// <summary>
    /// Backoff multiplier for calculating throttle delay.
    /// The delay is calculated as (excess bytes / limit) * factor.
    /// Default: 1.5x.
    /// </summary>
    [Range(1.0, 100.0)]
    public double ThrottleDelayFactor { get; set; } = 1.5;

    /// <inheritdoc />
    public IReadOnlyList<string> Validate()
    {
        var errors = new List<string>(ConfigValidator.ValidateDataAnnotations(this));

        // Validate nested per-client overrides
        foreach (var (clientId, quota) in ClientOverrides)
        {
            if (quota.ProduceBytesPerSec < 0)
                errors.Add($"{nameof(ClientOverrides)}[{clientId}].{nameof(ClientBandwidthQuota.ProduceBytesPerSec)}: must be >= 0.");
            if (quota.ConsumeBytesPerSec < 0)
                errors.Add($"{nameof(ClientOverrides)}[{clientId}].{nameof(ClientBandwidthQuota.ConsumeBytesPerSec)}: must be >= 0.");
        }
        foreach (var (userId, quota) in UserOverrides)
        {
            if (quota.ProduceBytesPerSec < 0)
                errors.Add($"{nameof(UserOverrides)}[{userId}].{nameof(ClientBandwidthQuota.ProduceBytesPerSec)}: must be >= 0.");
            if (quota.ConsumeBytesPerSec < 0)
                errors.Add($"{nameof(UserOverrides)}[{userId}].{nameof(ClientBandwidthQuota.ConsumeBytesPerSec)}: must be >= 0.");
        }

        return errors;
    }
}

/// <summary>
/// Bandwidth quota limits for a specific client or user.
/// </summary>
public sealed class ClientBandwidthQuota
{
    /// <summary>
    /// Maximum produce bytes per second. 0 = unlimited.
    /// </summary>
    [Range(0, long.MaxValue)]
    public long ProduceBytesPerSec { get; set; }

    /// <summary>
    /// Maximum consume (fetch) bytes per second. 0 = unlimited.
    /// </summary>
    [Range(0, long.MaxValue)]
    public long ConsumeBytesPerSec { get; set; }
}
