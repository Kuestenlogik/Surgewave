using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Core.Configuration;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Configuration for broker-native per-message TTL (time-to-live).
/// Messages with surgewave-ttl-ms headers are stored immediately but filtered
/// from fetch responses after their expiry time (baseTimestamp + ttlMs).
/// </summary>
public sealed class TtlConfig : IValidatableConfig
{
    /// <summary>
    /// Enable per-message TTL feature globally.
    /// Individual topics can opt-in via topic config "surgewave.ttl.enabled=true".
    /// </summary>
    public bool Enabled { get; set; } = false;

    /// <summary>
    /// Default TTL in milliseconds applied when no surgewave-ttl-ms header is present.
    /// 0 means no default TTL (messages never expire unless explicitly set).
    /// </summary>
    [Range(0, long.MaxValue)]
    public long DefaultTtlMs { get; set; } = 0;

    /// <summary>
    /// Maximum allowed TTL in milliseconds.
    /// Messages with a TTL exceeding this value will be capped to this maximum.
    /// Default: 7 days (604,800,000 ms).
    /// </summary>
    [Range(1, long.MaxValue)]
    public long MaxTtlMs { get; set; } = 7 * 24 * 60 * 60 * 1000L;

    /// <summary>
    /// Interval in milliseconds for sweeping expired TTL index entries.
    /// Default: 1000 ms (1 second).
    /// </summary>
    [Range(1, int.MaxValue)]
    public int IndexCleanupIntervalMs { get; set; } = 1000;

    /// <inheritdoc />
    public IReadOnlyList<string> Validate()
    {
        var errors = new List<string>(ConfigValidator.ValidateDataAnnotations(this));

        if (DefaultTtlMs > MaxTtlMs)
        {
            errors.Add($"{nameof(DefaultTtlMs)} ({DefaultTtlMs}ms) must not exceed " +
                       $"{nameof(MaxTtlMs)} ({MaxTtlMs}ms).");
        }

        return errors;
    }
}
