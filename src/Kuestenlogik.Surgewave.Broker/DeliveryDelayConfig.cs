using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Core.Configuration;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Configuration for broker-native delayed message delivery.
/// Messages with surgewave-deliver-at-ms or surgewave-deliver-after-ms headers
/// are stored immediately but filtered from fetch responses until their delivery time.
/// </summary>
public sealed class DeliveryDelayConfig : IValidatableConfig
{
    /// <summary>
    /// Enable delayed delivery feature globally.
    /// Individual topics can opt-in via topic config "surgewave.delay.enabled=true".
    /// </summary>
    public bool Enabled { get; set; } = false;

    /// <summary>
    /// Maximum allowed delay in milliseconds.
    /// Messages with a delay exceeding this value will be capped to this maximum.
    /// Default: 7 days (604,800,000 ms).
    /// </summary>
    [Range(1, long.MaxValue)]
    public long MaxDelayMs { get; set; } = 7 * 24 * 60 * 60 * 1000L;

    /// <summary>
    /// Interval in milliseconds for sweeping expired delay index entries.
    /// Default: 1000 ms (1 second).
    /// </summary>
    [Range(1, int.MaxValue)]
    public int IndexCleanupIntervalMs { get; set; } = 1000;

    /// <inheritdoc />
    public IReadOnlyList<string> Validate() => ConfigValidator.ValidateDataAnnotations(this);
}
