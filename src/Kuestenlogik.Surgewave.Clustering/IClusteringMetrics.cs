namespace Kuestenlogik.Surgewave.Clustering;

/// <summary>
/// Interface for recording clustering-related metrics.
/// Implemented by BrokerMetrics to avoid circular dependency.
/// </summary>
public interface IClusteringMetrics
{
    /// <summary>
    /// Record that a replica joined the ISR.
    /// </summary>
    void RecordReplicaJoinedIsr(string topic, int partition);

    /// <summary>
    /// Record that a replica left the ISR.
    /// </summary>
    void RecordReplicaLeftIsr(string topic, int partition);

    /// <summary>
    /// Record replication lag for a partition.
    /// </summary>
    void RecordReplicationLag(string topic, int partition, double lagMs);

    /// <summary>
    /// Record bytes replicated.
    /// </summary>
    void RecordReplicationBytes(string topic, int partition, long bytes);
}
