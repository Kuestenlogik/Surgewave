namespace Kuestenlogik.Surgewave.Broker.Quotas;

/// <summary>
/// Aggregate metrics for bandwidth quota enforcement.
/// </summary>
public sealed class BandwidthQuotaMetrics
{
    /// <summary>
    /// Total number of clients currently being tracked.
    /// </summary>
    public long TotalClientsTracked { get; init; }

    /// <summary>
    /// Number of clients currently exceeding their bandwidth quota.
    /// </summary>
    public long TotalClientsThrottled { get; init; }

    /// <summary>
    /// Total bytes that were subject to throttling decisions since startup.
    /// </summary>
    public long TotalBytesThrottled { get; init; }

    /// <summary>
    /// Total number of throttle events since startup.
    /// </summary>
    public long TotalThrottleEvents { get; init; }
}
