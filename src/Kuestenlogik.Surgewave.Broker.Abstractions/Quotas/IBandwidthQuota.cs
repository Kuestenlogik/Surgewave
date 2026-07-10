namespace Kuestenlogik.Surgewave.Broker.Quotas;

/// <summary>
/// Protocol-neutral seam over the per-client/per-user bandwidth quota manager.
/// Implemented by the broker's <c>BandwidthQuotaManager</c>.
/// </summary>
public interface IBandwidthQuota
{
    /// <summary>
    /// Whether bandwidth quotas are enabled.
    /// </summary>
    bool Enabled { get; }

    /// <summary>
    /// Check bandwidth and record bytes for a produce operation.
    /// Returns a throttle result indicating whether the request should be delayed.
    /// </summary>
    ThrottleResult CheckAndRecordProduce(string clientId, string? user, long bytes);

    /// <summary>
    /// Check bandwidth for a consume (fetch) operation without recording bytes.
    /// Use this for pre-flight throttle checks where actual bytes are not yet known.
    /// </summary>
    ThrottleResult CheckConsume(string clientId, string? user, long estimatedBytes);

    /// <summary>
    /// Record actual bytes consumed after a fetch operation completes.
    /// </summary>
    void RecordConsume(string clientId, long actualBytes);
}
