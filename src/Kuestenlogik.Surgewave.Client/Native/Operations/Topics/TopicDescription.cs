namespace Kuestenlogik.Surgewave.Client.Native.Operations.Topics;

/// <summary>
/// Detailed topic description including partition metadata.
/// </summary>
public record TopicDescription
{
    /// <summary>Topic name.</summary>
    public required string Name { get; init; }

    /// <summary>Number of partitions.</summary>
    public required int PartitionCount { get; init; }

    /// <summary>Replication factor.</summary>
    public required short ReplicationFactor { get; init; }

    /// <summary>Whether this is an internal topic.</summary>
    public required bool IsInternal { get; init; }

    /// <summary>Partition metadata.</summary>
    public required PartitionDescription[] Partitions { get; init; }
}

/// <summary>
/// Partition metadata including leader, replicas, and ISR.
/// </summary>
public record PartitionDescription
{
    /// <summary>Partition ID.</summary>
    public required int PartitionId { get; init; }

    /// <summary>Leader broker ID.</summary>
    public required int Leader { get; init; }

    /// <summary>Leader epoch.</summary>
    public required int LeaderEpoch { get; init; }

    /// <summary>Replica broker IDs.</summary>
    public required int[] Replicas { get; init; }

    /// <summary>In-sync replica broker IDs.</summary>
    public required int[] Isr { get; init; }

    /// <summary>High watermark offset.</summary>
    public required long HighWatermark { get; init; }

    /// <summary>Log start offset.</summary>
    public required long LogStartOffset { get; init; }
}
