namespace Kuestenlogik.Surgewave.Clustering.GeoReplication;

/// <summary>
/// State information for a mirrored topic.
/// </summary>
public sealed record MirrorTopicState
{
    public required string SourceTopic { get; init; }
    public required string LinkId { get; init; }
    public bool IsReadOnly { get; init; } = true;
    public int PartitionCount { get; init; }
    public Dictionary<int, long> ReplicationLag { get; init; } = new();
    public Dictionary<int, long> LastSyncedOffset { get; init; } = new();
}
