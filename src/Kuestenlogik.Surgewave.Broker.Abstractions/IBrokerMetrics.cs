namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Protocol-neutral broker-telemetry seam — exactly the members the data-plane handler records
/// on the produce / fetch paths (#59 b4-tier2). Optional: injected as <c>null</c> when metrics
/// are off. The concrete <c>BrokerMetrics</c> implements this alongside the clustering-metrics
/// contract.
/// </summary>
public interface IBrokerMetrics
{
    /// <summary>Record a deduplicated (rejected) message.</summary>
    void RecordDeduplication(string topic, int partition);

    /// <summary>Record a produce event (messages, bytes, latency).</summary>
    void RecordProduce(string topic, int partition, int messageCount, long bytes, double latencyMs);

    /// <summary>Record a produce error.</summary>
    void RecordProduceError(string topic, int partition, string errorCode);

    /// <summary>Record a fetch event (messages, bytes, latency).</summary>
    void RecordFetch(string topic, int partition, int messageCount, long bytes, double latencyMs);

    /// <summary>Record an error by type.</summary>
    void RecordError(string errorType);
}
