namespace Kuestenlogik.Surgewave.Broker.Quotas;

/// <summary>
/// Snapshot of bandwidth usage for a single client.
/// </summary>
public sealed class BandwidthUsage
{
    /// <summary>
    /// The client identifier.
    /// </summary>
    public required string ClientId { get; init; }

    /// <summary>
    /// Current produce rate in bytes per second.
    /// </summary>
    public long ProduceBytesPerSec { get; init; }

    /// <summary>
    /// Current consume rate in bytes per second.
    /// </summary>
    public long ConsumeBytesPerSec { get; init; }

    /// <summary>
    /// Configured produce limit in bytes per second. 0 = unlimited.
    /// </summary>
    public long ProduceLimitBytesPerSec { get; init; }

    /// <summary>
    /// Configured consume limit in bytes per second. 0 = unlimited.
    /// </summary>
    public long ConsumeLimitBytesPerSec { get; init; }

    /// <summary>
    /// Produce utilization percentage (current / limit * 100). 0 if unlimited.
    /// </summary>
    public double ProduceUtilizationPercent { get; init; }

    /// <summary>
    /// Consume utilization percentage (current / limit * 100). 0 if unlimited.
    /// </summary>
    public double ConsumeUtilizationPercent { get; init; }

    /// <summary>
    /// True if the client is currently being throttled on produce or consume.
    /// </summary>
    public bool IsThrottled { get; init; }

    /// <summary>
    /// Timestamp of last produce or consume activity.
    /// </summary>
    public DateTimeOffset LastActivityAt { get; init; }
}
