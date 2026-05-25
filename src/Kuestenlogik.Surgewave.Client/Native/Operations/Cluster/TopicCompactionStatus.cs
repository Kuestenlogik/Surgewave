namespace Kuestenlogik.Surgewave.Client.Native.Operations.Cluster;

/// <summary>
/// Compaction status for a topic.
/// </summary>
public record TopicCompactionStatus(string Topic, int PartitionCount, string CleanupPolicy, int SegmentCount, long TotalBytes);
