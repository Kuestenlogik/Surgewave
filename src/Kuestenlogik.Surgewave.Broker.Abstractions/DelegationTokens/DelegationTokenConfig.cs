namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Configuration for delegation tokens.
/// </summary>
public sealed class DelegationTokenConfig
{
    /// <summary>
    /// Whether delegation tokens are enabled. Default: false — opt-in security feature.
    /// </summary>
    public bool Enabled { get; set; } = false;

    /// <summary>
    /// Base64-encoded secret key for HMAC generation.
    /// If not set, a random key is generated (tokens won't survive restarts).
    /// </summary>
    public string? SecretKey { get; set; }

    /// <summary>
    /// Default token max lifetime in milliseconds. Default: 7 days.
    /// </summary>
    public long DefaultMaxLifetimeMs { get; set; } = 7 * 24 * 60 * 60 * 1000L;

    /// <summary>
    /// Maximum allowed max lifetime in milliseconds. Default: 7 days.
    /// </summary>
    public long MaxMaxLifetimeMs { get; set; } = 7 * 24 * 60 * 60 * 1000L;

    /// <summary>
    /// Default renewal period in milliseconds. Default: 24 hours.
    /// </summary>
    public long DefaultRenewalPeriodMs { get; set; } = 24 * 60 * 60 * 1000L;
}
