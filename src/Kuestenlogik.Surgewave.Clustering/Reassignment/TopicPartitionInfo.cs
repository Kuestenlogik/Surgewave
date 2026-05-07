namespace Kuestenlogik.Surgewave.Clustering.Reassignment;

/// <summary>
/// Describes the current state of a single topic-partition for reassignment planning.
/// </summary>
public sealed record TopicPartitionInfo(
    string Topic,
    int Partition,
    int Leader,
    IReadOnlyList<int> Replicas,
    IReadOnlyList<int> Isr,
    long SizeBytes);
