namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Quota configuration.
/// </summary>
public sealed class QuotaConfig
{
    /// <summary>
    /// Enable quota enforcement.
    /// </summary>
    public bool Enabled { get; set; } = false;

    /// <summary>
    /// Maximum bytes per second a producer client can send.
    /// -1 for unlimited.
    /// </summary>
    public long ProducerBytesPerSecond { get; set; } = -1;

    /// <summary>
    /// Maximum burst bytes for producer (token bucket capacity).
    /// </summary>
    public long ProducerBurstBytes { get; set; } = 104857600; // 100MB

    /// <summary>
    /// Maximum bytes per second a consumer client can fetch.
    /// -1 for unlimited.
    /// </summary>
    public long ConsumerBytesPerSecond { get; set; } = -1;

    /// <summary>
    /// Maximum burst bytes for consumer (token bucket capacity).
    /// </summary>
    public long ConsumerBurstBytes { get; set; } = 104857600; // 100MB

    /// <summary>
    /// Maximum throttle time in milliseconds.
    /// </summary>
    public int MaxThrottleTimeMs { get; set; } = 30000; // 30 seconds

    /// <summary>
    /// Time in minutes after which inactive client state is cleaned up.
    /// </summary>
    public int ClientInactivityTimeoutMinutes { get; set; } = 10;

    public static QuotaConfig Default => new();

    public static QuotaConfig Disabled => new() { Enabled = false };
}
