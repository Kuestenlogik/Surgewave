namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Protocol-neutral seam over the client request/fetch quota manager (token-bucket
/// produce/fetch byte-rate limiting). Implemented by the broker's <c>QuotaManager</c>.
/// </summary>
public interface IQuotaManager
{
    /// <summary>
    /// Check if a produce request should be throttled.
    /// Returns throttle time in milliseconds (0 if not throttled).
    /// </summary>
    int CheckProduceQuota(string clientId, long bytesToProduce);

    /// <summary>
    /// Check if a fetch request should be throttled.
    /// Returns throttle time in milliseconds (0 if not throttled).
    /// </summary>
    int CheckFetchQuota(string clientId, long bytesToFetch);

    /// <summary>
    /// Record actual bytes produced (for tracking after successful produce).
    /// </summary>
    void RecordProducedBytes(string clientId, long bytes);

    /// <summary>
    /// Record actual bytes fetched (for tracking after successful fetch).
    /// </summary>
    void RecordFetchedBytes(string clientId, long bytes);

    /// <summary>
    /// Get a copy of the current configuration.
    /// </summary>
    QuotaConfig Config { get; }

    /// <summary>
    /// Get quota statistics for a client.
    /// </summary>
    ClientQuotaStats? GetClientStats(string clientId);

    /// <summary>
    /// Get all client quota statistics.
    /// </summary>
    IEnumerable<(string ClientId, ClientQuotaStats Stats)> GetAllClientStats();

    /// <summary>
    /// Update quota configuration and persist to disk.
    /// </summary>
    void UpdateConfig(
        bool? enabled = null,
        long? producerBytesPerSecond = null,
        long? producerBurstBytes = null,
        long? consumerBytesPerSecond = null,
        long? consumerBurstBytes = null,
        int? maxThrottleTimeMs = null,
        int? clientInactivityTimeoutMinutes = null);
}
