namespace Kuestenlogik.Surgewave.Hosting;

/// <summary>
/// Topic configuration options.
/// </summary>
public sealed class TopicOptions
{
    /// <summary>
    /// Default number of partitions for new topics.
    /// Default: 1.
    /// </summary>
    public int DefaultPartitions { get; set; } = 1;

    /// <summary>
    /// Default replication factor for new topics.
    /// Default: 1.
    /// </summary>
    public short DefaultReplicationFactor { get; set; } = 1;

    /// <summary>
    /// Minimum in-sync replicas required for writes.
    /// Default: 1.
    /// </summary>
    public int MinInSyncReplicas { get; set; } = 1;
}
