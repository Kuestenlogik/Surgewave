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

    /// <summary>
    /// Record how many offsets a SINGLE replication fetch ingested.
    /// </summary>
    /// <remarks>
    /// The shape of a follower's catch-up, which bytes alone do not show: one fetch carrying two
    /// hundred offsets and two hundred fetches carrying one each move the same bytes and mean
    /// very different things — the first says the follower fell behind and caught up in bulk, the
    /// second that it kept pace. Worth having in production for that reason, and it is also the
    /// only honest way for a test to establish that the multi-batch path was entered at all
    /// (#177 follow-up): the answer used to be read out of a Debug log line, which made a test
    /// verdict depend on a log level an operator is free to change.
    /// </remarks>
    void RecordReplicationFetch(string topic, int partition, long offsets);
}
